const fs = require('fs');

const extract = require('extract-zip');
const { ObjectID } = require('bson');

const config = require('./config');
const { connectDB, storeSchema, setParsingResult, removeItemFromQueue } = require('./db');
const files = require('./files');
const schema = require('./schema');


config.verifyConfig();

let isDBConnected = false;

// TODO: Refactor and move these into db.js
let sourcesBucket, parsingQueue;

connectDB('contracts_scan', 'sources', 'schemas', 'parsing_results').then((dbInfo) => {
    sourcesBucket = dbInfo.sourcesBucket;
    parsingQueue = dbInfo.parsingQueue;

    isDBConnected = true;
    console.info('connected to database');
}).catch((reason) => {
    console.error('failed to connect to database ', reason);
    throw reason;
});

const workLoop = async () => {
    let extractPath, sourceID, queueItem;

    try {
        if (isDBConnected === false) {
            console.log('not connected');
            return;
        }

        if (await parsingQueue.size() == 0) {
            return;
        }

        queueItem = await parsingQueue.get();
        sourceID = new ObjectID(queueItem.payload);
        
        const cursor = await sourcesBucket.find({ _id: sourceID });
        const entries = await cursor.toArray();
        
        if (entries.length == 0) {
            throw `source ${sourceID} not found`;
        }

        const contractAddress = entries[0]['metadata']['address'];
    
        const sourceSavePath = files.getSourceSavePath();
        
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
        
        let projectPath = extractPath;

        if ('crateName' in entries[0]['metadata']) {
            projectPath = schema.getCratePath(extractPath, entries[0]['metadata']['crateName']);
        }

        let res = await schema.getSchemaInfo(projectPath);

        if (res.msgs.length === 0) {
            console.log(`no messages found for processing in ${res.crateName}`);
            return;
        }

        schema.patchCargo(projectPath);
        schema.generateSchema(projectPath, res.msgs);
        schema.executeSchema(projectPath);
        
        const schemas = await storeSchema(projectPath, sourceID, contractAddress, res.msgs);
        await setParsingResult(sourceID, {
            schemas: schemas,
            parsed: true,
        });

        console.log(`Successfully parsed ${sourceID}`);

    } catch (e) {
        console.error(`processing failed: ${e}`);

        try {
            await setParsingResult(sourceID, { error: JSON.stringify(e) });
        } catch (e) {
            console.error(e);
        }

    } finally {

        if (queueItem) {
            await removeItemFromQueue(sourceID, queueItem);
        }

        setTimeout(workLoop, Number(process.env.QUEUE_CHECK_INTERVAL));

        if (extractPath) {
            files.cleanup(extractPath);
        }
    }
}

setTimeout(workLoop, Number(process.env.QUEUE_CHECK_INTERVAL));
