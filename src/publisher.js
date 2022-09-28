const { PubSub } = require('@google-cloud/pubsub');
const { removeItemFromPublishQueue } = require('./db');
const pubSubClient = new PubSub();

module.exports = async (publishQueue) => {
    while (await publishQueue.size() > 0) {
        try {
            const queueItem = await publishQueue.get();
            const dataBuffer = Buffer.from(JSON.stringify(queueItem.payload));

            const messageId = await pubSubClient
                .topic(process.env.PUB_SUB_TOPIC_ID)
                .publishMessage({ data: dataBuffer });

            console.log(`Message ${messageId} published.`);

            await removeItemFromPublishQueue(queueItem);
        } catch (e) {
            console.error(`Received error while publishing: ${e.message}`);
        }
    }
}