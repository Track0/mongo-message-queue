# TypeScript/ESM Modernization Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Rewrite mongo-message-queue as a modern TypeScript ESM library with proper types, native JS replacements for lodash, dual CJS/ESM output, vitest tests, graceful shutdown, EventEmitter error handling, and recommended MongoDB indexes.

**Architecture:** Single `MessageQueue` class using private `#fields` for encapsulation, extending `EventEmitter` for error/event handling. Built with `tsup` for dual ESM/CJS output. Source in `src/`, output in `dist/`. Tests via `vitest` with `mongodb-memory-server` for integration tests.

**Tech Stack:** TypeScript 5.8+, tsup, vitest, mongodb-memory-server, mongodb driver >=6 <8, Node 18+

---

### Task 1: Project Scaffolding — package.json, tsconfig, tsup, vitest

**Files:**
- Modify: `package.json`
- Create: `tsconfig.json`
- Create: `tsup.config.ts`
- Create: `vitest.config.ts`
- Modify: `.gitignore`
- Modify: `.npmignore`

**Step 1: Update package.json**

Replace the entire `package.json` with:

```json
{
  "name": "mongo-message-queue",
  "version": "2.0.0",
  "description": "A promise based message queue for Node.js using a mongodb backing store.",
  "type": "module",
  "exports": {
    ".": {
      "import": {
        "types": "./dist/index.d.ts",
        "default": "./dist/index.js"
      },
      "require": {
        "types": "./dist/index.d.cts",
        "default": "./dist/index.cjs"
      }
    }
  },
  "main": "./dist/index.cjs",
  "module": "./dist/index.js",
  "types": "./dist/index.d.ts",
  "files": [
    "dist"
  ],
  "scripts": {
    "build": "tsup",
    "test": "vitest run",
    "test:watch": "vitest",
    "typecheck": "tsc --noEmit",
    "prepublishOnly": "npm run build"
  },
  "peerDependencies": {
    "mongodb": ">= 6.0.0 < 8"
  },
  "devDependencies": {
    "mongodb": "^7.1.0",
    "mongodb-memory-server": "^10.4.0",
    "tsup": "^8.4.0",
    "typescript": "^5.8.0",
    "vitest": "^3.1.0"
  },
  "engines": {
    "node": ">=18"
  },
  "repository": {
    "type": "git",
    "url": "https://github.com/wonderlic/mongo-message-queue.git"
  },
  "license": "MIT"
}
```

**Step 2: Create tsconfig.json**

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ES2022",
    "moduleResolution": "bundler",
    "lib": ["ES2022"],
    "outDir": "dist",
    "rootDir": "src",
    "declaration": true,
    "declarationMap": true,
    "sourceMap": true,
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true,
    "resolveJsonModule": true,
    "isolatedModules": true
  },
  "include": ["src"],
  "exclude": ["node_modules", "dist", "test"]
}
```

**Step 3: Create tsup.config.ts**

```typescript
import { defineConfig } from 'tsup';

export default defineConfig({
  entry: ['src/index.ts'],
  format: ['esm', 'cjs'],
  dts: true,
  clean: true,
  sourcemap: true,
});
```

**Step 4: Create vitest.config.ts**

```typescript
import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    testTimeout: 15_000,
    hookTimeout: 30_000,
  },
});
```

**Step 5: Update .gitignore**

```
node_modules/
dist/
.idea/
.env
.npmrc
```

**Step 6: Update .npmignore**

Replace with (since we use `"files": ["dist"]` in package.json, .npmignore is redundant but keep it clean):

```
.idea/
node_modules/
src/
test/
docs/
.npmrc
tsconfig.json
tsup.config.ts
vitest.config.ts
```

**Step 7: Install dependencies**

Run: `npm install`
Expected: Clean install with all dev deps resolved.

**Step 8: Commit**

```bash
git add package.json tsconfig.json tsup.config.ts vitest.config.ts .gitignore .npmignore
git commit -m "chore: scaffold TypeScript/ESM project with tsup and vitest"
```

---

### Task 2: TypeScript Types and Interfaces

**Files:**
- Create: `src/types.ts`

**Step 1: Create the types file**

```typescript
import type { Collection, Db, Document, ObjectId } from 'mongodb';

export type WorkerStatus = 'Completed' | 'Retry' | 'Rejected';

export interface QueueItem<T extends Document = Document> {
  _id: ObjectId;
  dateCreated: Date;
  type: string;
  message: T;
  priority: number;
  receivedTime?: Date;
  nextReceivableTime?: Date;
  retryCount?: number;
  rejectedTime?: Date;
  rejectionReason?: string;
  releasedReason?: string;
  releaseHistory?: ReleaseHistoryEntry[];
}

export interface ReleaseHistoryEntry {
  retryCount: number;
  receivedTime?: Date;
  releasedTime: Date;
  releasedReason?: string;
}

export interface EnqueueOptions {
  nextReceivableTime?: Date;
  /** Priority 1-10, where 1 is highest priority. Defaults to 1. */
  priority?: number;
}

export interface UpdateOptions {
  nextReceivableTime?: Date;
}

export type WorkerFunction<T extends Document = Document> = (
  queueItem: QueueItem<T>,
) => Promise<WorkerStatus>;

export type DatabaseProvider = () => Promise<Db>;

export interface MessageQueueEvents {
  error: [error: Error];
}
```

**Step 2: Commit**

```bash
git add src/types.ts
git commit -m "feat: add TypeScript type definitions for queue items, workers, and events"
```

---

### Task 3: Core MessageQueue Class — TypeScript/ESM with Private Fields

This is the main rewrite. Converts the constructor function to a class with `#private` fields, drops lodash, removes dead compat shims, uses `EventEmitter`, adds `AbortController`-based graceful shutdown, replaces `setImmediate` with `setTimeout(fn, 0)`, and adds a static method for recommended indexes.

**Files:**
- Create: `src/message-queue.ts`
- Create: `src/index.ts`
- Delete: `lib/message-queue.js` (after task is done)
- Delete: `index.js` (after task is done)

**Step 1: Create src/message-queue.ts**

```typescript
import { EventEmitter } from 'node:events';
import type { Collection, Db, Document } from 'mongodb';
import type {
  DatabaseProvider,
  EnqueueOptions,
  MessageQueueEvents,
  QueueItem,
  UpdateOptions,
  WorkerFunction,
  WorkerStatus,
} from './types.js';

export class MessageQueue extends EventEmitter<MessageQueueEvents> {
  databasePromise: DatabaseProvider | null = null;
  collectionName = '_queue';
  pollingInterval = 1_000;
  processingTimeout = 30_000;
  maxWorkers = 5;

  #workers = new Map<string, WorkerFunction>();
  #numWorkers = 0;
  #pollingIntervalId: ReturnType<typeof setInterval> | null = null;
  #abortController: AbortController | null = null;

  registerWorker(type: string, handler: WorkerFunction): void {
    this.#workers.set(type, handler);
    this.#startPolling();
  }

  stopPolling(): void {
    this.#stopPolling();
  }

  async drain(): Promise<void> {
    this.#stopPolling();

    // Wait for all in-flight workers to finish
    while (this.#numWorkers > 0) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
  }

  async enqueue<T extends Document = Document>(
    type: string,
    message: T,
    options: EnqueueOptions = {},
  ): Promise<QueueItem<T>> {
    const queueItem: Omit<QueueItem<T>, '_id'> = {
      dateCreated: new Date(),
      type,
      message,
      priority: options.priority ?? 1,
    };

    if (options.nextReceivableTime) {
      queueItem.nextReceivableTime = options.nextReceivableTime;
    }

    return this.#enqueue(queueItem);
  }

  async enqueueAndProcess<T extends Document = Document>(
    type: string,
    message: T,
  ): Promise<void> {
    const queueItem: Omit<QueueItem<T>, '_id'> = {
      dateCreated: new Date(),
      type,
      message,
      priority: 1,
      receivedTime: new Date(),
    };

    const inserted = await this.#enqueue(queueItem);
    await this.#process(inserted);
  }

  async removeOne(
    type: string,
    messageQuery: Record<string, unknown>,
  ): Promise<number> {
    const query = this.#buildQueueItemQuery(type, messageQuery);
    return this.#removeOne(query);
  }

  async removeMany(
    type: string,
    messageQuery: Record<string, unknown>,
  ): Promise<number> {
    const query = this.#buildQueueItemQuery(type, messageQuery);
    return this.#removeMany(query);
  }

  async updateOne(
    type: string,
    messageQuery: Record<string, unknown>,
    messageUpdate: Record<string, unknown>,
    options: UpdateOptions = {},
  ): Promise<number> {
    const query = this.#buildQueueItemQuery(type, messageQuery);
    const update = this.#buildQueueItemUpdate(messageUpdate, options);
    return this.#updateOne(query, update);
  }

  async updateMany(
    type: string,
    messageQuery: Record<string, unknown>,
    messageUpdate: Record<string, unknown>,
    options: UpdateOptions = {},
  ): Promise<number> {
    const query = this.#buildQueueItemQuery(type, messageQuery);
    const update = this.#buildQueueItemUpdate(messageUpdate, options);
    return this.#updateMany(query, update);
  }

  /**
   * Creates the recommended indexes for the queue collection.
   * Call this once during application startup.
   */
  async createIndexes(): Promise<void> {
    const collection = await this.#getCollection();
    await collection.createIndex(
      {
        type: 1,
        rejectedTime: 1,
        nextReceivableTime: 1,
        receivedTime: 1,
        priority: 1,
      },
      { name: 'queue_receive_idx' },
    );
  }

  // --- Private methods ---

  #startPolling(): void {
    if (!this.#pollingIntervalId) {
      this.#abortController = new AbortController();
      this.#pollingIntervalId = setInterval(
        () => this.#poll(),
        this.pollingInterval,
      );
    }
  }

  #stopPolling(): void {
    if (this.#pollingIntervalId) {
      clearInterval(this.#pollingIntervalId);
      this.#pollingIntervalId = null;
    }
    if (this.#abortController) {
      this.#abortController.abort();
      this.#abortController = null;
    }
  }

  async #poll(): Promise<void> {
    if (this.#numWorkers >= this.maxWorkers) return;

    this.#numWorkers += 1;
    try {
      const queueItem = await this.#receive();
      if (queueItem) {
        await this.#process(queueItem);
        // Look for more work immediately if we just processed something
        setTimeout(() => this.#poll(), 0);
      }
    } catch (err) {
      this.emit('error', err instanceof Error ? err : new Error(String(err)));
    } finally {
      this.#numWorkers -= 1;
    }
  }

  async #process(queueItem: QueueItem): Promise<void> {
    const worker = this.#workers.get(queueItem.type);
    if (!worker) {
      throw new Error(`No worker registered for type: ${queueItem.type}`);
    }

    const status: WorkerStatus = await worker(queueItem);
    switch (status) {
      case 'Completed':
        await this.#removeOneById(queueItem._id);
        return;
      case 'Retry':
        await this.#release(queueItem);
        return;
      case 'Rejected':
        await this.#reject(queueItem);
        return;
      default:
        throw new Error(`Unknown status: ${status as string}`);
    }
  }

  async #enqueue(queueItem: Omit<QueueItem, '_id'>): Promise<QueueItem> {
    const collection = await this.#getCollection();
    const result = await collection.insertOne(queueItem);
    return { ...queueItem, _id: result.insertedId } as QueueItem;
  }

  async #release(queueItem: QueueItem): Promise<void> {
    const update = {
      $unset: { receivedTime: '' as const },
      $set: {
        retryCount: (queueItem.retryCount ?? 0) + 1,
        nextReceivableTime: queueItem.nextReceivableTime ?? new Date(),
      },
      $push: {
        releaseHistory: {
          retryCount: queueItem.retryCount ?? 0,
          receivedTime: queueItem.receivedTime,
          releasedTime: new Date(),
          releasedReason: queueItem.releasedReason,
        },
      },
    };

    await this.#updateOneById(queueItem._id, update);
  }

  async #reject(queueItem: QueueItem): Promise<void> {
    const update = {
      $unset: {
        receivedTime: '' as const,
        nextReceivableTime: '' as const,
      },
      $set: {
        rejectedTime: new Date(),
        rejectionReason: queueItem.rejectionReason,
      },
      $push: {
        releaseHistory: {
          retryCount: queueItem.retryCount ?? 0,
          receivedTime: queueItem.receivedTime,
          releasedTime: new Date(),
          releasedReason: queueItem.releasedReason,
        },
      },
    };

    await this.#updateOneById(queueItem._id, update);
  }

  async #receive(): Promise<QueueItem | null> {
    const types = [...this.#workers.keys()];
    if (types.length === 0) return null;

    const query = {
      type: { $in: types },
      rejectedTime: { $exists: false },
      $and: [
        {
          $or: [
            { nextReceivableTime: { $lt: new Date() } },
            { nextReceivableTime: { $exists: false } },
          ],
        },
        {
          $or: [
            {
              receivedTime: {
                $lt: new Date(Date.now() - this.processingTimeout),
              },
            },
            { receivedTime: { $exists: false } },
          ],
        },
      ],
    };

    const update = {
      $set: { receivedTime: new Date() },
    };

    const collection = await this.#getCollection();
    const result = await collection.findOneAndUpdate(query, update, {
      returnDocument: 'after',
      sort: { priority: 1 },
    });

    return (result as QueueItem) ?? null;
  }

  #buildQueueItemQuery(
    type: string,
    messageQuery: Record<string, unknown>,
  ): Record<string, unknown> {
    const query: Record<string, unknown> = { type };

    for (const [key, value] of Object.entries(messageQuery)) {
      query[`message.${key}`] = value;
    }

    return query;
  }

  #buildQueueItemUpdate(
    messageUpdate: Record<string, unknown>,
    options: UpdateOptions,
  ): Record<string, unknown> {
    const $set: Record<string, unknown> = {};

    if (options.nextReceivableTime) {
      $set.nextReceivableTime = options.nextReceivableTime;
    }

    for (const [key, value] of Object.entries(messageUpdate)) {
      $set[`message.${key}`] = value;
    }

    if (Object.keys($set).length === 0) {
      return {};
    }

    return { $set };
  }

  async #getCollection(): Promise<Collection> {
    if (!this.databasePromise) {
      throw new Error('No database configured');
    }
    const db: Db = await this.databasePromise();
    return db.collection(this.collectionName);
  }

  async #removeOne(query: Record<string, unknown>): Promise<number> {
    const collection = await this.#getCollection();
    const result = await collection.deleteOne(query);
    return result.deletedCount;
  }

  async #removeOneById(id: unknown): Promise<number> {
    return this.#removeOne({ _id: id });
  }

  async #removeMany(query: Record<string, unknown>): Promise<number> {
    const collection = await this.#getCollection();
    const result = await collection.deleteMany(query);
    return result.deletedCount;
  }

  async #updateOne(
    query: Record<string, unknown>,
    update: Record<string, unknown>,
  ): Promise<number> {
    const collection = await this.#getCollection();
    const result = await collection.updateOne(query, update);
    return result.modifiedCount;
  }

  async #updateOneById(
    id: unknown,
    update: Record<string, unknown>,
  ): Promise<number> {
    return this.#updateOne({ _id: id }, update);
  }

  async #updateMany(
    query: Record<string, unknown>,
    update: Record<string, unknown>,
  ): Promise<number> {
    const collection = await this.#getCollection();
    const result = await collection.updateMany(query, update);
    return result.modifiedCount;
  }
}
```

**Step 2: Create src/index.ts**

```typescript
export { MessageQueue } from './message-queue.js';
export type {
  DatabaseProvider,
  EnqueueOptions,
  MessageQueueEvents,
  QueueItem,
  ReleaseHistoryEntry,
  UpdateOptions,
  WorkerFunction,
  WorkerStatus,
} from './types.js';
```

**Step 3: Verify it compiles**

Run: `npx tsc --noEmit`
Expected: No errors.

**Step 4: Verify it builds**

Run: `npm run build`
Expected: `dist/` folder created with `index.js`, `index.cjs`, `index.d.ts`, `index.d.cts`.

**Step 5: Commit**

```bash
git add src/
git commit -m "feat: rewrite MessageQueue as TypeScript ESM class with private fields

- Convert constructor function to ES2022 class with #private fields
- Drop lodash dependency entirely (native replacements)
- Remove dead MongoDB driver v3 compat shims
- Extend EventEmitter for error handling
- Add AbortController-based drain() for graceful shutdown
- Replace setImmediate with setTimeout(fn, 0)
- Add createIndexes() for recommended compound index
- Return proper QueueItem with _id from enqueue()
- Type all public API surfaces"
```

---

### Task 4: Delete Old JS Source Files

**Files:**
- Delete: `lib/message-queue.js`
- Delete: `index.js`

**Step 1: Remove old files**

Run:
```bash
rm lib/message-queue.js index.js
rmdir lib
```

**Step 2: Commit**

```bash
git add -A
git commit -m "chore: remove old CommonJS source files"
```

---

### Task 5: Tests — Setup and Enqueue/Dequeue

**Files:**
- Create: `test/setup.ts`
- Create: `test/enqueue.test.ts`

**Step 1: Create test/setup.ts**

This is the shared test helper that starts/stops `mongodb-memory-server`.

```typescript
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
```

**Step 2: Create test/enqueue.test.ts**

```typescript
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
```

**Step 3: Run tests**

Run: `npm test`
Expected: All tests pass.

**Step 4: Commit**

```bash
git add test/
git commit -m "test: add enqueue, remove, and update integration tests with mongodb-memory-server"
```

---

### Task 6: Tests — Worker Processing, Retry, Reject

**Files:**
- Create: `test/worker.test.ts`

**Step 1: Create test/worker.test.ts**

```typescript
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
```

**Step 2: Run tests**

Run: `npm test`
Expected: All tests pass.

**Step 3: Commit**

```bash
git add test/worker.test.ts
git commit -m "test: add worker processing, retry, reject, drain, and priority tests"
```

---

### Task 7: Tests — createIndexes

**Files:**
- Create: `test/indexes.test.ts`

**Step 1: Create test/indexes.test.ts**

```typescript
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
```

**Step 2: Run tests**

Run: `npm test`
Expected: All tests pass.

**Step 3: Commit**

```bash
git add test/indexes.test.ts
git commit -m "test: add createIndexes integration test"
```

---

### Task 8: Update README for v2

**Files:**
- Modify: `README.md`

**Step 1: Rewrite README.md**

Replace with updated content that reflects:
- ESM import syntax (`import { MessageQueue } from 'mongo-message-queue'`)
- TypeScript usage examples
- `async/await` in all examples (no `.then()` chains)
- New `drain()` method
- New `createIndexes()` method
- `EventEmitter` error handling (`queue.on('error', ...)`)
- Updated compatibility (MongoDB driver 6-7, Node 18+)
- Exported types

```markdown
# mongo-message-queue

[![NPM version](https://badge.fury.io/js/mongo-message-queue.svg)](http://badge.fury.io/js/mongo-message-queue)

### A typed, promise-based message queue for Node.js backed by MongoDB.

## Compatibility

- **Node.js:** 18+
- **MongoDB driver:** 6.x, 7.x
- **MongoDB server:** 5 through 8

## Installation

```
npm install mongo-message-queue
```

## Usage

### Import & Create

```typescript
import { MessageQueue } from 'mongo-message-queue';

const queue = new MessageQueue();
```

### Database Configuration

Set `.databasePromise` to a function that returns a `Promise<Db>`:

```typescript
import { MongoClient } from 'mongodb';

const client = new MongoClient('mongodb://localhost:27017');
await client.connect();

queue.databasePromise = () => Promise.resolve(client.db('myapp'));
```

### Create Recommended Indexes

Call once during application startup:

```typescript
await queue.createIndexes();
```

### Error Handling

`MessageQueue` extends `EventEmitter`. Listen for errors:

```typescript
queue.on('error', (err) => {
  console.error('Queue error:', err);
});
```

### Register Workers

```typescript
queue.registerWorker('doSomething', async (queueItem) => {
  try {
    await db.collection('results').updateOne(
      { _id: queueItem.message.id },
      { $set: { status: queueItem.message.status } },
    );
    return 'Completed';
  } catch (err) {
    queueItem.releasedReason = (err as Error).message;
    if ((queueItem.retryCount ?? 0) < 5) {
      queueItem.nextReceivableTime = new Date(Date.now() + 30_000);
      return 'Retry';
    }
    queueItem.rejectionReason = 'Gave up after 5 retries.';
    return 'Rejected';
  }
});
```

### Enqueue Messages

```typescript
await queue.enqueue('doSomething', { id: 123, status: 'done' });

// With a delay:
await queue.enqueue('doSomething', { id: 123 }, {
  nextReceivableTime: new Date(Date.now() + 30_000),
});

// With priority (1 = highest, 10 = lowest):
await queue.enqueue('doSomething', { id: 123 }, { priority: 5 });

// Enqueue and process immediately with a local worker:
await queue.enqueueAndProcess('doSomething', { id: 123, status: 'done' });
```

### Graceful Shutdown

```typescript
// Stop polling and wait for in-flight workers to finish:
await queue.drain();
```

### Configuration

| Property | Default | Description |
|---|---|---|
| `collectionName` | `'_queue'` | MongoDB collection name |
| `pollingInterval` | `1000` | Milliseconds between polls |
| `processingTimeout` | `30000` | Milliseconds before a message is re-offered |
| `maxWorkers` | `5` | Maximum concurrent workers |

### Query & Update Messages

```typescript
await queue.removeOne('type', { id: 123 });
await queue.removeMany('type', { group: 'a' });
await queue.updateOne('type', { id: 123 }, { status: 'new' });
await queue.updateMany('type', { group: 'a' }, { status: 'new' }, {
  nextReceivableTime: new Date(Date.now() + 60_000),
});
```

### TypeScript Types

All types are exported:

```typescript
import type {
  QueueItem,
  WorkerFunction,
  WorkerStatus,
  EnqueueOptions,
  UpdateOptions,
  DatabaseProvider,
} from 'mongo-message-queue';
```

## License

(The MIT License)

Copyright (c) 2014-2026 Wonderlic, Inc. <SoftwareDevelopment@wonderlic.com>

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
'Software'), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
```

**Step 2: Commit**

```bash
git add README.md
git commit -m "docs: update README for v2 with TypeScript, ESM, and new API surface"
```

---

### Task 9: Final Verification

**Step 1: Clean build**

Run: `rm -rf dist && npm run build`
Expected: Build succeeds, `dist/` contains `index.js`, `index.cjs`, `index.d.ts`, `index.d.cts`.

**Step 2: Type check**

Run: `npm run typecheck`
Expected: No errors.

**Step 3: Full test suite**

Run: `npm test`
Expected: All tests pass.

**Step 4: Verify exports work for ESM**

Run: `node -e "import('mongo-message-queue').then(m => console.log(Object.keys(m)))" --input-type=module`

Or create a quick smoke test:
Run: `node --input-type=module -e "import { MessageQueue } from './dist/index.js'; const q = new MessageQueue(); console.log('ESM OK:', typeof q.enqueue)"`
Expected: `ESM OK: function`

**Step 5: Verify exports work for CJS**

Run: `node -e "const { MessageQueue } = require('./dist/index.cjs'); const q = new MessageQueue(); console.log('CJS OK:', typeof q.enqueue)"`
Expected: `CJS OK: function`

**Step 6: Final commit if any adjustments were needed**

---

## Summary of All Changes

| # | What | Why |
|---|---|---|
| 1 | TypeScript + ESM + dual CJS/ESM via tsup | Modern library standard |
| 2 | Drop lodash | 4 trivial uses replaced with native JS |
| 3 | Upgrade mongodb peer dep to >=6 <8, Node >=18 | Support latest driver, drop EOL Node |
| 4 | ES2022 class with #private fields | Replace self=this closure pattern |
| 5 | Remove dead driver v3 compat code | peer dep requires v6+, shims are unreachable |
| 6 | Full TypeScript types for public API | QueueItem, WorkerStatus, EnqueueOptions, etc. |
| 7 | `exports` field in package.json | Proper dual ESM/CJS resolution |
| 8 | Replace setImmediate with setTimeout(fn, 0) | Standards-compliant, more portable |
| 9 | AbortController + drain() for graceful shutdown | Clean shutdown that waits for in-flight work |
| 10 | EventEmitter for errors | Standard Node pattern vs bare errorHandler property |
| 11 | vitest + mongodb-memory-server tests | 15+ integration tests covering all public API |
| 12 | tsconfig.json + strict TypeScript | Type safety and IDE support |
| 13 | createIndexes() method | Addresses the TODO that's been there since day one |
