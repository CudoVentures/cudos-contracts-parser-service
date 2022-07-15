const fs = require('fs');
const { EOL } = require('os');
const path = require('path');
const childProc = require('child_process');
const toml = require('toml');
const _ = require('lodash');

const files = require('./files');


const ATTRIBUTE_PATH = ['fn', 'attrs', 0, 'path', 'segments', 0, 'ident'];

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
    const jsonFilePath = filePath.replace('.rs', '.json');

    try {
        childProc.execSync(`${process.env.RUST_2_JSON_BIN_PATH} ${filePath} ${jsonFilePath}`, {
            timeout: Number(process.env.RUST_2_JSON_TIMEOUT),
        });
    } catch (e) {
        console.error(e);
        throw e.stderr.toString();
    }
    
    return JSON.parse(fs.readFileSync(jsonFilePath, 'utf8'));
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

module.exports.patchCargo = (projectPath) => {
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

module.exports.generateSchema = (projectPath, msgs) => {
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
        fs.rmSync(schemaPath, { recursive: true, force: true });
    }
}

module.exports.getSchemaInfo = async (projectPath) => {
    const parsedCargo = parseCargo(projectPath);
    const crateName = getCrateName(parsedCargo);

    let projectFiles = await files.getFiles(projectPath);

    projectFiles = files.filterRustSourceCodeFiles(projectFiles)
    projectFiles = files.filterFullPaths(projectFiles)

    let msgs = [];

    projectFiles.forEach((file) => {
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
                usePath: funcMsgTypeUsePath,
                funcName: funcName,
            });
        }
    });

    return {
        crateName: crateName,
        msgs: msgs
    }
}

module.exports.executeSchema = (projectPath) => {
    try {
        childProc.execSync(`cargo schema`, {
            cwd: path.join(projectPath, 'examples'),
            timeout: Number(process.env.EXEC_SCHEMA_TIMEOUT),
        });
    } catch (e) {
        console.error(e);
        throw e.stderr.toString();
    }
}

module.exports.getCratePath = (projectPath, crateName) => {
    let output;

    try {
        output = childProc.execSync(`cargo pkgid ${crateName}`, {
            cwd: projectPath,
            timeout: 10000,
        });
    } catch (e) {
        console.error(e);
        throw e.stderr.toString();
    }

    output = output.toString();
    output = output.replace('file://', '');

    return output.substring(0, output.lastIndexOf('#'));
}