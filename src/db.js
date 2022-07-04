const fs  = require('fs');
const path = require('path');

const { MongoClient, GridFSBucket } = require('mongodb');
const mongoDbQueue = require('@openwar/mongodb-queue');

const files = require('./files');


let parsingResultsCollection, schemasBucket;

module.exports.setParsingResultError = async (sourceID, err) => {
    parsingResultsCollection.updateOne({ '_id': sourceID }, {
        error: err,
    }).catch((e) => {
        console.error(`failed setting error for ${sourceID} to ${err} with error: ${e}`);
    });
}

module.exports.storeSchema = async (projectPath, sourceID) => {
    let schemaFiles = await files.getFiles(path.join(projectPath, 'examples', 'schema', '/'));

    schemaFiles = schemaFiles.filter((value) => {
        if (value['name'].endsWith('.json')) {
            return value;
        }
    });

    const timestamp = Math.floor(new Date().getTime() / 1000);

    let schemasIDs = [];

    for (const file of schemaFiles) {
        const filename = file['name'];

        const uploadStream = schemasBucket.openUploadStream(`${timestamp}-${filename}`, {
            sourceID: sourceID,
            timestamp: timestamp
            // TODO: We can add username here
        });
        
        const buffer = fs.readFileSync(file['path']);

        uploadStream.write(buffer);
        uploadStream.end();
    
        schemasIDs.push(uploadStream.id.toString());
    }

    parsingResultsCollection.updateOne({ '_id': sourceID }, { $set: {
        schemas: schemasIDs,
        parsed: true,
    }});
}

module.exports.connectDB = async (dbName, sourcesBucketName, schemasBucketName, parsingResultsCollName) => {
    const client = await MongoClient.connect(process.env.MONGO_URI, {
        connectTimeoutMS: 10000, // Give up initial connection after 10 seconds
        socketTimeoutMS: 45000, // Close sockets after 45 seconds of inactivity
    });

    const db = client.db(dbName);
    
    parsingResultsCollection = db.collection(parsingResultsCollName);
    schemasBucket = new GridFSBucket(db, {bucketName: schemasBucketName});

    return {
        sourcesBucket: new GridFSBucket(db, {bucketName: sourcesBucketName}),
        parsingQueue: mongoDbQueue(db, 'parsing-queue', {
            visibility: Number(process.env.QUEUE_ITEM_VISIBILITY),
        })
    };
}