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
