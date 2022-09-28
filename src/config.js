require('dotenv').config();


const CONFIG_KEYS = ['SOURCES_SAVE_PATH', 'MONGO_URI', 'QUEUE_ITEM_VISIBILITY', 'QUEUE_CHECK_INTERVAL', 
    'EXEC_SCHEMA_TIMEOUT', 'RUST_2_JSON_BIN_PATH', 'RUST_2_JSON_TIMEOUT', 'PUB_SUB_TOPIC_ID'];

module.exports.verifyConfig = () => {
    for (const configKey of CONFIG_KEYS) {
        if (!process.env[configKey]) {
            throw `config value '${configKey}' is not set`;
        }
    }
}