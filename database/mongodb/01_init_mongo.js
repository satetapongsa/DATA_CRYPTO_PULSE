// =============================================================================
// MONGODB INITIALIZATION SCRIPT - RAW DATA LAKEHOUSE
// =============================================================================

const dbName = process.env.MONGO_INITDB_DATABASE || 'crypto_lakehouse';
const db = db.getSiblingDB(dbName);

print(`Initializing MongoDB database: ${dbName}...`);

// 1. Create Raw Crypto Market Collection
if (!db.getCollectionNames().includes('raw_crypto_market')) {
    db.createCollection('raw_crypto_market');
    print('Created collection: raw_crypto_market');
}
db.raw_crypto_market.createIndex({ "symbol": 1, "timestamp": -1 });
db.raw_crypto_market.createIndex({ "batch_id": 1 });
db.raw_crypto_market.createIndex({ "ingested_at": -1 });

// 2. Create Raw News Feed Collection
if (!db.getCollectionNames().includes('raw_news_feed')) {
    db.createCollection('raw_news_feed');
    print('Created collection: raw_news_feed');
}
db.raw_news_feed.createIndex({ "symbol": 1 });
db.raw_news_feed.createIndex({ "published_at": -1 });
db.raw_news_feed.createIndex({ "batch_id": 1 });

// 3. Create Pipeline Audit Logs Collection
if (!db.getCollectionNames().includes('pipeline_audit_logs')) {
    db.createCollection('pipeline_audit_logs');
    print('Created collection: pipeline_audit_logs');
}
db.pipeline_audit_logs.createIndex({ "run_id": 1 });
db.pipeline_audit_logs.createIndex({ "dag_id": 1 });
db.pipeline_audit_logs.createIndex({ "execution_timestamp": -1 });

print('MongoDB Initialization completed successfully.');
