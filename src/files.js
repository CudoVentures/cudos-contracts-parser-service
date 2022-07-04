const fs = require('fs');
const path = require('path');
const { v4: uuidv4 } = require('uuid');


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

module.exports.getFiles = getFiles;

module.exports.filterRustSourceCodeFiles = (files) => {
    return files.filter((value) => {
        if (value['name'].endsWith('.rs') && value['name'] != 'schema.rs') {
            return value;
        }
    });
}

module.exports.filterFullPaths = (files) => {
    return files.map((value) => {
        return value['path'];
    });
}

module.exports.getSourceSavePath = () => {
    let fullPath;

    do {
        fullPath = path.join(process.env.SOURCES_SAVE_PATH, uuidv4());
    } while(fs.existsSync(fullPath));

    fs.mkdirSync(fullPath);

    return path.join(fullPath, 'source.zip');
}

module.exports.cleanup = (projectPath) => {
    try {
        fs.rmSync(projectPath, { recursive: true, force: true });
    } catch (e) {
        console.error(`cleanup failed ${e}`);
    }
}