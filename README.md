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
