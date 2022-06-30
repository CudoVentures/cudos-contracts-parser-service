const { EOL } = require('os');
const path = require('path');
const fs = require('fs');
const childProc = require('child_process');

const toml = require('toml');
const _ = require('lodash');
const extract = require('extract-zip')
const { MongoClient, GridFSBucket } = require('mongodb');
const mongoDbQueue = require('@openwar/mongodb-queue');
const { ObjectID } = require('bson');
const { v4: uuidv4 } = require('uuid');

const SOURCES_SAVE_PATH = '/tmp';
const MONGO_URI = 'mongodb://root:toor@127.0.0.1/contracts_scan?retryWrites=true&w=1';
const QUEUE_ITEM_VISIBILITY = 15 * 60; // 15mins
const QUEUE_CHECK_INTERVAL = 2000; // 2 seconds
const EXEC_SCHEMA_TIMEOUT = 10 * 60; // 10mins

let isDBConnected = false;

const getFiles = async (iteratePath) => {
    const entries = fs.readdirSync(iteratePath, { withFileTypes: true });

    const files = entries
        .filter(file => !file.isDirectory())
        .map(file => ({ ...file, path: iteratePath + file.name }));

    const folders = entries.filter(folder => folder.isDirectory());

    for (const folder of folders) {
        const nextPath =  path.normalize(path.join(iteratePath, folder.name, '/'));
        files.push(...await getFiles(nextPath));
    }

    return files;
}

const filterRustSourceCodeFiles = (files) => {
    return files.filter((value) => {
        if (value['name'].endsWith('.rs') && value['name'] != 'schema.rs') {
            return value;
        }
    });
}

const filterFullPaths = (files) => {
    return files.map((value) => {
        return value['path'];
    });
}

const parseCargo = (projectPath) => {
    const cargoData = fs.readFileSync(path.join(projectPath, 'Cargo.toml'));
    return toml.parse(cargoData.toString());
}

const getCrateName = (parsedCargo) => {
    return parsedCargo['package']['name'].replaceAll('-', '_');
}

const hasDevDependencies = (parsedCargo) => {
    return 'dev-dependencies' in parsedCargo;
}

const hasDevDependency = (parsedCargo, dependencyName) => {
    return dependencyName in parsedCargo['dev-dependencies'];
}

const parseSource = (filePath) => {
    return JSON.parse(fs.readFileSync(filePath.replace('.rs', '.json'), 'utf8'));
}

const getUsePath = (useObj, typeName, prevPath) => {
    if ('tree' in useObj) {
        return getUsePath(useObj['tree'], typeName, prevPath);
    }

    if ('path' in useObj) {
        return getUsePath(useObj['path'], typeName, prevPath + useObj['path']['ident'] + '::');
    }

    if ('group' in useObj) {
        for (const item of useObj['group']) {
            if (item['ident'] == typeName) {
                return prevPath + typeName;
            }
        }
    }
}

const getTypeUsePath = (parsedSource, typeName) => {
    for (const item of parsedSource['items']) {
        if ('use' in item) {
            const usePath = getUsePath(item['use'], typeName, '');

            if (usePath) {
                return usePath;
            }
        }
    }
}

const ATTRIBUTE_PATH = ['fn', 'attrs', 0, 'path', 'segments', 0, 'ident'];

const getEntryFuncMsgType = (parsedSource, funcName) => {
    let msgType;

    for (const item of parsedSource['items']) {
        if (_.get(item, ['fn', 'vis']) != 'pub' || _.get(item, ['fn', 'ident']) != funcName || _.get(item, ATTRIBUTE_PATH) != 'entry_point') {
            continue;
        }

        const inputs = _.get(item, ['fn', 'inputs']);
        const input = inputs[inputs.length - 1];
        
        if (!_.has(input, ['typed', 'ty', 'path', 'segments', 0, 'ident'])) {
            continue;
        }

        msgType = _.get(input, ['typed', 'ty', 'path', 'segments', 0, 'ident']);
    }

    if (msgType) {
        return msgType;
    }
}

const patchCargo = (projectPath) => {
    const parsedCargo = parseCargo(projectPath);
    const cargoPath = path.join(projectPath, 'Cargo.toml');

    if (!hasDevDependencies(parsedCargo)) {
        fs.appendFileSync(cargoPath, `${EOL}${EOL}[dev-dependencies]`);
    }

    if (!hasDevDependency(parsedCargo, 'cosmwasm-schema')) {
        const cargo = fs.readFileSync(cargoPath).toString();
        const devDependenciesPos = cargo.indexOf('[dev-dependencies]');

        cargo = cargo.slice(0, devDependenciesPos) + `${EOL}cosmwasm-schema = { version = "1.0.0" }` + cargo.slice(devDependenciesPos);

        fs.writeFileSync(cargoPath, cargo);
    }
}

const generateUses = (msgs) => {
    let uses = '';

    for (const msg of msgs) {
        uses += `${EOL}use ${msg.usePath};`;
    }

    return uses;
}

const generateExportCalls = (msgs) => {
    let exportCalls = '', i = 0;

    for (const msg of msgs) {
        exportCalls += `${EOL}\texport_schema(&schema_for!(${msg.type}), &out_dir);`;
    }

    return exportCalls;
}

const generateSchema = (projectPath, msgs) => {
    const uses = generateUses(msgs);
    const exportCalls = generateExportCalls(msgs);

    const schemaTemplate = fs.readFileSync('schema-template.rs').toString();
    const schema = schemaTemplate.replace('USES_PLACEHOLDER', uses).replace('EXPORTS_CALLS_PLACEHOLDER', exportCalls);

    let schemaPath = path.join(projectPath, 'examples');

    if (!fs.existsSync(schemaPath)) {
        fs.mkdirSync(schemaPath);
    }

    const schemaRsPath = path.join(schemaPath, 'schema.rs');

    if (fs.existsSync(schemaRsPath)) {
        fs.unlinkSync(schemaRsPath);
    }

    fs.writeFileSync(schemaRsPath, schema);

    schemaPath = path.join(schemaPath, 'schema');

    if (fs.existsSync(schemaPath)) {
        fs.rm(schemaPath, { recursive: true });
    }
}

const getSchemaInfo = async (projectPath) => {
    const parsedCargo = parseCargo(projectPath);
    const crateName = getCrateName(parsedCargo);

    let files = await getFiles(projectPath);

    files = filterRustSourceCodeFiles(files)
    files = filterFullPaths(files)

    let msgs = [];

    files.forEach((file) => {
        const source = parseSource(file);

        for (const funcName of ['execute', 'query']) {
            const funcMsgType = getEntryFuncMsgType(source, funcName);
            
            if (!funcMsgType) {
                return;
            }

            let funcMsgTypeUsePath = getTypeUsePath(source, funcMsgType);

            if (!funcMsgTypeUsePath) {
                return;
            }

            if (funcMsgTypeUsePath.startsWith('crate')) {
                funcMsgTypeUsePath = funcMsgTypeUsePath.replace('crate', crateName);
            }

            msgs.push({
                type: funcMsgType,
                usePath: funcMsgTypeUsePath
            });
        }
    });

    return {
        crateName: crateName,
        msgs: msgs
    }
}

const executeSchema = (projectPath) => {
    try {
        childProc.execSync(`cargo schem`, {
            cwd: path.join(projectPath, 'examples'),
            timeout: EXEC_SCHEMA_TIMEOUT,
        });
        console.log('output ', output);
    } catch (e) {
        throw e.stderr.toString();
    }
}

const storeSchema = (projectPath) => {

}

const cleanup = (projectPath) => {
    
}

const connectDB = async (dbName, sourcesBucketName, parsingResultsCollName) => {
    const client = await MongoClient.connect(MONGO_URI, {
        connectTimeoutMS: 10000, // Give up initial connection after 10 seconds
        socketTimeoutMS: 45000, // Close sockets after 45 seconds of inactivity
    });

    const db = client.db(dbName);

    const parsingResults = db.collection(parsingResultsCollName);

    return {
        db: db,
        sourcesBucket: new GridFSBucket(db, {bucketName: sourcesBucketName}),
        parsingResultsCollection: parsingResults
    };
}

let sourcesBucket, parsingQueue, parsingResultsCollection;

connectDB('contracts_scan', 'sources', 'parsing_results').then((result) => {

    sourcesBucket = result.sourcesBucket;
    parsingResultsCollection = result.parsingResultsCollection;

    parsingQueue = mongoDbQueue(result.db, 'parsing-queue', {
        visibility: QUEUE_ITEM_VISIBILITY, // 15mins
    });

    isDBConnected = true;

    console.info('connected to database');
}).catch((reason) => {
    console.error('failed to connect to database ', reason);
    throw reason;
});

const getSourceSavePath = () => {
    let fullPath;

    do {
        fullPath = path.join(SOURCES_SAVE_PATH, uuidv4());
    } while(fs.existsSync(fullPath));

    fs.mkdirSync(fullPath);

    return path.join(fullPath, 'source.zip');
}

const process = async () => {
    let extractPath = '';

    try {
        if (isDBConnected === false) {
            console.log('not connected');
            return;
        }

        if (await parsingQueue.size() == 0) {
            console.log('nothing in queue');
            return;
        }

        const queueItem = await parsingQueue.get();
        const sourceID = new ObjectID(queueItem.payload);
        
        const cursor = await sourcesBucket.find({ _id: sourceID });
        const entries = await cursor.toArray();
        
        if (entries.length == 0) {
            throw `source ${sourceID} not found`;
        }
    
        const sourceSavePath = getSourceSavePath();
        
        await new Promise((resolve, reject) => {
            const stream = fs.createWriteStream(sourceSavePath);
            stream.on('finish', () => { resolve(); });
            stream.on('error', (e) => { reject(e); });

            const downloadStream = sourcesBucket.openDownloadStream(sourceID);
            downloadStream.on('error', (e) => { reject(e); });
            downloadStream.pipe(stream);
        });

        extractPath = sourceSavePath.replace('source.zip', '');

        await extract(sourceSavePath, { dir: extractPath });

        console.log(`extracted to ${extractPath}`);

        getSchemaInfo(extractPath).then((res) => {
            if (res.msgs.length === 0) {
                console.log(`no messages found for processing in ${res.crateName}`);
                return;
            }

            patchCargo(extractPath);
            generateSchema(extractPath, res.msgs);
            executeSchema(extractPath);
            storeSchema(extractPath);
            cleanup(extractPath);

        }).catch((e) => {
            console.error(e);
        });

    } catch (e) {
        console.error(`processing failed: ${e}`);
    } finally {
        setTimeout(process, QUEUE_CHECK_INTERVAL);

        try {
            cleanup(extractPath);
        } catch (e) {
            console.error(`cleanup failed ${e}`);
        }
    }
}

setTimeout(process, QUEUE_CHECK_INTERVAL);
