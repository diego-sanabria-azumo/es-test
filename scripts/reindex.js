const { MongoClient } = require('mongodb');
const mysql = require('mysql2/promise');
const { Client: ESClient } = require('@elastic/elasticsearch');
const dotenv = require('dotenv');

dotenv.config();

const BATCH_SIZE = 100;
const es = new ESClient({ node: process.env.ES_HOST });

async function connectMongo() {
  const mongo = new MongoClient(process.env.MONGO_URI);
  await mongo.connect();
  return {
    advisorCollection: mongo.db('vb').collection('advisor_activity_feed'),
    timelineCollection: mongo.db('vb').collection('timeline'),
  };
}

async function connectMySQL() {
  return await mysql.createPool({
    host: process.env.MYSQL_HOST,
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASSWORD,
    database: process.env.MYSQL_DATABASE,
    waitForConnections: true,
    connectionLimit: 10,
  });
}

async function fetchUser(mysqlPool, userId) {
  const [rows] = await mysqlPool.query('SELECT * FROM vb_user WHERE id = ?', [userId]);
  return rows[0] || null;
}

async function runReindex() {
  const { advisorCollection, timelineCollection } = await connectMongo();
  //const mysqlPool = await connectMySQL();

  const total = 100;
  //const total = await advisorCollection.countDocuments();
  console.log(`Found ${total} documents to reindex.`);

  for (let skip = 0; skip < total; skip += BATCH_SIZE) {
    const advisorDocs = await advisorCollection.find({})
      .skip(skip)
      .limit(100)
      .toArray();

    const bulkOps = [];

    for (const doc of advisorDocs) {
      const timeline = await timelineCollection.findOne({ _id: doc.timeline });
      //const user = await fetchUser(mysqlPool, doc.user_id);

      const enriched = {
        user_id: doc.user_id ? doc.user_id.toString('hex') : null,
        company_id: doc.company_id ? doc.company_id.toString('hex') : null,
        advisor_id: doc.advisor_id ? doc.advisor_id.toString('hex') : null,
        type: doc.type,
        timeline: timeline
          ? {
              ...timeline,
              user_id: timeline.user_id ? timeline.user_id.toString('hex') : null,
              company_id: timeline.company_id ? timeline.company_id.toString('hex') : null,
              advisor_id: timeline.advisor_id ? timeline.advisor_id.toString('hex') : null,
              created_at: timeline.created_at ? timeline.created_at.toISOString() : null,
            }
          : {},
        status: doc.status,
        createdAt: doc.createdAt ? doc.createdAt.toISOString() : null,
      };

      bulkOps.push({ index: { _index: 'advisor_activity_feed_enriched', _id: doc._id.toString() } });
      bulkOps.push(enriched);
    }

    if (bulkOps.length > 0) {
      const res = await es.bulk({ body: bulkOps });

      if (res.errors) {
        console.error('❌ Bulk insert had errors:', JSON.stringify(res.items, null, 2));
      } else {
        console.log(`✅ Indexed ${bulkOps.length / 2} docs`);
      }
    }
  }

  console.log('🎉 Reindex complete');
  process.exit(0);
}

runReindex().catch(console.error);
