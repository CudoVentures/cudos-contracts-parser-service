const fs  = require('fs');
const path = require('path');

const { MongoClient, GridFSBucket } = require('mongodb');
const mongoDbQueue = require('@openwar/mongodb-queue');

const files = require('./files');


let parsingQueue, parsingResultsCollection, schemasBucket;


module.exports.setParsingResult = async (sourceID, result) => {
    try {
        await parsingResultsCollection.updateOne({ _id: sourceID.toString() }, { $set: result });
    } catch (e) {
        throw `failed setting result for ${sourceID} to ${result} with error: ${e}`;
    }
}

module.exports.removeItemFromQueue = async (sourceID, queueItem) => {
    try {
        // TODO: Remove item from queue only after X tries
        await parsingQueue.ack(queueItem.ack);
    } catch (e) {
        console.error(`failed to acknowledge ${sourceID} ${queueItem.ack}`);
    }
}

module.exports.storeSchema = async (projectPath, sourceID, contractAddress, msgs) => {
    let schemaFiles = await files.getFiles(path.join(projectPath, 'examples', 'schema', '/'));

    schemaFiles = schemaFiles.filter((value) => {
        if (value['name'].endsWith('.json')) {
            return value;
        }
    });

    const timestamp = getTimestamp();

    let schemas = [];

    for (const file of schemaFiles) {
        const filename = file['name'];

        const funcName = matchFilenameToEntryFuncName(filename, msgs);

        if (!funcName) {
            throw `could not find entry function name for schema file ${filename}`;
        }

        const uploadStream = schemasBucket.openUploadStream(`${timestamp}-${filename}`, {
            sourceID: sourceID,
            address: contractAddress,
            funcName: funcName,
            timestamp: timestamp,
            // TODO: We can add username here
        });

        const buffer = fs.readFileSync(file['path']);

        uploadStream.write(buffer);
        uploadStream.end();

        schemas.push({
            id: uploadStream.id.toString(),
            funcName: funcName,
        });
    }

    return schemas;
}

module.exports.connectDB = async (dbName, sourcesBucketName, schemasBucketName, parsingResultsCollName) => {
    const client = await MongoClient.connect(process.env.MONGO_URI, {
        connectTimeoutMS: 10000, // Give up initial connection after 10 seconds
        socketTimeoutMS: 45000, // Close sockets after 45 seconds of inactivity
    });

    const db = client.db(dbName);
    
    parsingQueue = mongoDbQueue(db, 'parsing-queue', {
        visibility: Number(process.env.QUEUE_ITEM_VISIBILITY),
    });
    parsingResultsCollection = db.collection(parsingResultsCollName);
    schemasBucket = new GridFSBucket(db, {bucketName: schemasBucketName});

    return {
        sourcesBucket: new GridFSBucket(db, {bucketName: sourcesBucketName}),
        parsingQueue: parsingQueue,
    };
}

const getTimestamp = () => {
    return Math.floor(new Date().getTime() / 1000);
}

// TODO: Can be made smarter

const matchFilenameToEntryFuncName = (filename, msgs) => {
    for (const msg of msgs) {
        if (msg['type'].toUpperCase() == filename.replace('.json', '').replaceAll('_', '').toUpperCase()) {
            return msg['funcName'];
        }
    }
}