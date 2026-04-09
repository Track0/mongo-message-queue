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

describe('createIndexes', () => {
  it('creates the recommended compound index', async () => {
    await queue.createIndexes();

    const db = getDb();
    const indexes = await db.collection(queue.collectionName).indexes();
    const queueIdx = indexes.find((idx) => idx.name === 'queue_receive_idx');

    expect(queueIdx).toBeDefined();
    expect(queueIdx!.key).toEqual({
      type: 1,
      rejectedTime: 1,
      nextReceivableTime: 1,
      receivedTime: 1,
      priority: 1,
    });
  });

  it('is idempotent', async () => {
    await queue.createIndexes();
    await queue.createIndexes(); // Should not throw
  });
});
