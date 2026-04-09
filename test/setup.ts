import { MongoMemoryServer } from 'mongodb-memory-server';
import { MongoClient, type Db } from 'mongodb';
import { MessageQueue } from '../src/index.js';

let mongod: MongoMemoryServer;
let client: MongoClient;
let db: Db;

export async function setupMongo(): Promise<void> {
  mongod = await MongoMemoryServer.create();
  const uri = mongod.getUri();
  client = new MongoClient(uri);
  await client.connect();
  db = client.db('test_queue');
}

export async function teardownMongo(): Promise<void> {
  await client?.close();
  await mongod?.stop();
}

export function getDb(): Db {
  return db;
}

export function createQueue(): MessageQueue {
  const queue = new MessageQueue();
  queue.databasePromise = () => Promise.resolve(db);
  queue.collectionName = '_queue_test';
  return queue;
}

export async function clearQueue(queue: MessageQueue): Promise<void> {
  const collection = db.collection(queue.collectionName);
  await collection.deleteMany({});
}
