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

    return this.#enqueue(queueItem) as Promise<QueueItem<T>>;
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
