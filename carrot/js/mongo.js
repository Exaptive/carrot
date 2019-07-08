'use strict';

const MongoClient = require('mongodb').MongoClient;

const ObjectId = require('mongodb').ObjectID;

let _connection = null;

function connect(config) {
  return new Promise((resolve, reject) => {
    MongoClient.connect(config.url, config.options, (err, client) => {
      if (err) {
        reject(err);
        return;
      }
      resolve(client);
    });
  });
}

async function getConnection(config) {
  if (_connection) return _connection;
  _connection = await connect(config);
  return _connection;
}

async function closeConnection() {
  if (_connection) await _connection.close();
  _connection = null;
}

async function getDB({ config, dbName }) {
  const client = await getConnection(config);
  return client.db(dbName);
}

async function getCollection({ config, dbName, collectionName }) {
  const db = await getDB({ config, dbName });
  const collection = await db.collection(collectionName);
  return collection;
}

module.exports = {
  closeConnection,
  getCollection,
  ObjectId,
};
