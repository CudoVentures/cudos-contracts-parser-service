const fs = require('fs');

const extract = require('extract-zip');
const { ObjectID } = require('bson');

const config = require('./config');
const db = require('./db');
const files = require('./files');
const schema = require('./schema');


config.verifyConfig();

let isDBConnected = false;

// TODO: Refactor and move these into db.js
let sourcesBucket, parsingQueue;

db.connectDB('contracts_scan', 'sources', 'schemas', 'parsing_results').then((dbInfo) => {
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
            console.log('nothing in queue');
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

        let res = await schema.getSchemaInfo(extractPath);

        if (res.msgs.length === 0) {
            console.log(`no messages found for processing in ${res.crateName}`);
            return;
        }

        schema.patchCargo(extractPath);
        schema.generateSchema(extractPath, res.msgs);
        schema.executeSchema(extractPath);
        await db.storeSchema(extractPath, sourceID, contractAddress, res.msgs);

        console.log(`Successfully parsed ${sourceID}`);

    } catch (e) {
        console.error(`processing failed: ${e}`);

        await db.setParsingResultError(sourceID, JSON.stringify(e));

    } finally {

        if (queueItem) {
            try {
                // TODO: Remove item from queue only after X tries
                await parsingQueue.ack(queueItem.ack);
            } catch (e) {
                console.error(`failed to acknowledge ${sourceID} ${queueItem.ack}`);
            }
        }

        setTimeout(workLoop, Number(process.env.QUEUE_CHECK_INTERVAL));

        if (extractPath) {
            files.cleanup(extractPath);
        }
    }
}

setTimeout(workLoop, Number(process.env.QUEUE_CHECK_INTERVAL));
