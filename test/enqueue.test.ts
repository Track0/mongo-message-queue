import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { setupMongo, teardownMongo, createQueue, clearQueue, getDb } from './setup.js';
import type { MessageQueue } from '../src/index.js';

let queue: MessageQueue;

beforeAll(async () => {
  await setupMongo();
});

afterAll(async () => {
  await teardownMongo();
});

beforeEach(async () => {
  queue = createQueue();
  await clearQueue(queue);
});

describe('enqueue', () => {
  it('inserts a message and returns a QueueItem with _id', async () => {
    const result = await queue.enqueue('test', { foo: 'bar' });

    expect(result._id).toBeDefined();
    expect(result.type).toBe('test');
    expect(result.message).toEqual({ foo: 'bar' });
    expect(result.priority).toBe(1);
    expect(result.dateCreated).toBeInstanceOf(Date);
  });

  it('respects priority option', async () => {
    const result = await queue.enqueue('test', { a: 1 }, { priority: 5 });
    expect(result.priority).toBe(5);
  });

  it('respects nextReceivableTime option', async () => {
    const future = new Date(Date.now() + 60_000);
    const result = await queue.enqueue('test', { a: 1 }, { nextReceivableTime: future });
    expect(result.nextReceivableTime).toEqual(future);
  });

  it('persists to the database', async () => {
    const result = await queue.enqueue('test', { persisted: true });
    const db = getDb();
    const doc = await db.collection(queue.collectionName).findOne({ _id: result._id });
    expect(doc).not.toBeNull();
    expect(doc!.message).toEqual({ persisted: true });
  });

  it('throws when no database is configured', async () => {
    const noDbQueue = createQueue();
    noDbQueue.databasePromise = null;
    await expect(noDbQueue.enqueue('test', {})).rejects.toThrow('No database configured');
  });
});

describe('removeOne / removeMany', () => {
  it('removeOne deletes a single matching message', async () => {
    await queue.enqueue('rm', { id: 1 });
    await queue.enqueue('rm', { id: 2 });

    const count = await queue.removeOne('rm', { id: 1 });
    expect(count).toBe(1);

    const db = getDb();
    const remaining = await db.collection(queue.collectionName).countDocuments({ type: 'rm' });
    expect(remaining).toBe(1);
  });

  it('removeMany deletes all matching messages', async () => {
    await queue.enqueue('rm', { group: 'a' });
    await queue.enqueue('rm', { group: 'a' });
    await queue.enqueue('rm', { group: 'b' });

    const count = await queue.removeMany('rm', { group: 'a' });
    expect(count).toBe(2);
  });
});

describe('updateOne / updateMany', () => {
  it('updateOne modifies a single matching message', async () => {
    await queue.enqueue('up', { id: 1, status: 'pending' });

    const count = await queue.updateOne('up', { id: 1 }, { status: 'done' });
    expect(count).toBe(1);

    const db = getDb();
    const doc = await db.collection(queue.collectionName).findOne({ 'message.id': 1 });
    expect(doc!.message.status).toBe('done');
  });

  it('updateMany modifies all matching messages', async () => {
    await queue.enqueue('up', { group: 'x', status: 'old' });
    await queue.enqueue('up', { group: 'x', status: 'old' });

    const count = await queue.updateMany('up', { group: 'x' }, { status: 'new' });
    expect(count).toBe(2);
  });

  it('updateOne sets nextReceivableTime from options', async () => {
    await queue.enqueue('up', { id: 99 });
    const future = new Date(Date.now() + 60_000);

    await queue.updateOne('up', { id: 99 }, {}, { nextReceivableTime: future });

    const db = getDb();
    const doc = await db.collection(queue.collectionName).findOne({ 'message.id': 99 });
    expect(doc!.nextReceivableTime).toEqual(future);
  });
});
