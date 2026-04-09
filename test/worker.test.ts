import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from 'vitest';
import { setupMongo, teardownMongo, createQueue, clearQueue, getDb } from './setup.js';
import type { MessageQueue, QueueItem } from '../src/index.js';

let queue: MessageQueue;

beforeAll(async () => {
  await setupMongo();
});

afterAll(async () => {
  await teardownMongo();
});

beforeEach(async () => {
  queue = createQueue();
  queue.pollingInterval = 100; // Fast polling for tests
  await clearQueue(queue);
});

afterEach(async () => {
  await queue.drain();
});

describe('worker processing', () => {
  it('processes and removes a Completed message', async () => {
    const processed: unknown[] = [];

    queue.registerWorker('job', async (item: QueueItem) => {
      processed.push(item.message);
      return 'Completed';
    });

    await queue.enqueue('job', { task: 'do-it' });

    // Wait for processing
    await new Promise((resolve) => setTimeout(resolve, 500));

    expect(processed).toEqual([{ task: 'do-it' }]);

    const db = getDb();
    const remaining = await db.collection(queue.collectionName).countDocuments();
    expect(remaining).toBe(0);
  });

  it('retries a message and increments retryCount', async () => {
    let attempts = 0;

    queue.registerWorker('retry-job', async (item: QueueItem) => {
      attempts++;
      if (attempts < 3) {
        item.releasedReason = 'not ready';
        return 'Retry';
      }
      return 'Completed';
    });

    await queue.enqueue('retry-job', { data: 1 });

    // Wait for retries
    await new Promise((resolve) => setTimeout(resolve, 2000));

    expect(attempts).toBe(3);

    const db = getDb();
    const remaining = await db.collection(queue.collectionName).countDocuments();
    expect(remaining).toBe(0);
  });

  it('rejects a message and sets rejectedTime', async () => {
    queue.registerWorker('reject-job', async (item: QueueItem) => {
      item.rejectionReason = 'bad data';
      return 'Rejected';
    });

    await queue.enqueue('reject-job', { bad: true });

    await new Promise((resolve) => setTimeout(resolve, 500));

    const db = getDb();
    const doc = await db.collection(queue.collectionName).findOne({ type: 'reject-job' });
    expect(doc).not.toBeNull();
    expect(doc!.rejectedTime).toBeInstanceOf(Date);
    expect(doc!.rejectionReason).toBe('bad data');
  });

  it('emits error event when worker throws', async () => {
    const errors: Error[] = [];
    queue.on('error', (err) => errors.push(err));

    queue.registerWorker('fail-job', async () => {
      throw new Error('boom');
    });

    await queue.enqueue('fail-job', {});

    await new Promise((resolve) => setTimeout(resolve, 500));

    expect(errors.length).toBeGreaterThan(0);
    expect(errors[0].message).toBe('boom');
  });
});

describe('enqueueAndProcess', () => {
  it('processes immediately without waiting for poll', async () => {
    const processed: unknown[] = [];

    queue.registerWorker('immediate', async (item: QueueItem) => {
      processed.push(item.message);
      return 'Completed';
    });

    await queue.enqueueAndProcess('immediate', { instant: true });

    expect(processed).toEqual([{ instant: true }]);
  });
});

describe('drain', () => {
  it('stops polling and waits for in-flight workers', async () => {
    let completed = false;

    queue.registerWorker('slow', async () => {
      await new Promise((resolve) => setTimeout(resolve, 200));
      completed = true;
      return 'Completed';
    });

    await queue.enqueue('slow', {});

    // Let polling pick it up
    await new Promise((resolve) => setTimeout(resolve, 200));

    await queue.drain();
    expect(completed).toBe(true);
  });
});

describe('priority ordering', () => {
  it('processes higher priority (lower number) messages first', async () => {
    const order: number[] = [];

    // Enqueue low priority first, high priority second
    await queue.enqueue('prio', { n: 3 }, { priority: 3 });
    await queue.enqueue('prio', { n: 1 }, { priority: 1 });
    await queue.enqueue('prio', { n: 2 }, { priority: 2 });

    queue.maxWorkers = 1; // Force serial processing
    queue.registerWorker('prio', async (item: QueueItem) => {
      order.push((item.message as { n: number }).n);
      return 'Completed';
    });

    await new Promise((resolve) => setTimeout(resolve, 1500));
    await queue.drain();

    expect(order).toEqual([1, 2, 3]);
  });
});
