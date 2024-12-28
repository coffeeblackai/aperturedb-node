import { BaseClient } from './base.js';
import { QueryCommand } from './query.js';
import { Logger } from './utils/logger.js';

export interface ParallelQueryOptions {
  maxBatchSize?: number;
  maxConcurrentBatches?: number;
  retryAttempts?: number;
  retryDelayMs?: number;
}

export interface QueryExecutor {
  query<T = any>(q: any, blobs: Buffer[]): Promise<[T, Buffer[]]>;
}

export class ParallelQuery {
  private query: QueryCommand[];
  private blobs: Buffer[];
  private retryAttempts: number;
  private retryDelayMs: number;
  private executed: boolean = false;
  private response: any = null;
  private responseBlobs: Buffer[] = [];
  private error: Error | null = null;

  constructor(query: QueryCommand[], blobs: Buffer[] = [], options: ParallelQueryOptions = {}) {
    this.query = query;
    this.blobs = blobs;
    this.retryAttempts = options.retryAttempts ?? 3;
    this.retryDelayMs = options.retryDelayMs ?? 1000;
  }

  async execute(client: QueryExecutor): Promise<[any, Buffer[]]> {
    if (this.executed) {
      if (this.error) throw this.error;
      return [this.response, this.responseBlobs];
    }

    let lastError: Error | null = null;
    for (let attempt = 0; attempt <= this.retryAttempts; attempt++) {
      try {
        [this.response, this.responseBlobs] = await client.query(this.query, this.blobs);
        
        // Check if the response indicates an error
        if (this.response && typeof this.response === 'object') {
          if ('status' in this.response && this.response.status < 0) {
            throw new Error(this.response.info || 'Query failed with negative status');
          }
          if ('info' in this.response && this.response.info === 'Not Authenticated!') {
            throw new Error('Authentication failed during query execution');
          }
        }
        
        this.executed = true;
        return [this.response, this.responseBlobs];
      } catch (error) {
        lastError = error instanceof Error ? error : new Error('Unknown error during query execution');
        if (attempt < this.retryAttempts) {
          // Exponential backoff with jitter
          const delay = this.retryDelayMs * Math.pow(2, attempt) * (0.5 + Math.random());
          await new Promise(resolve => setTimeout(resolve, delay));
          continue;
        }
      }
    }

    this.executed = true;
    this.error = lastError ?? new Error('Query failed after retries');
    throw this.error;
  }

  getResponse(): any {
    return this.response;
  }

  getBlobs(): Buffer[] {
    return this.responseBlobs;
  }

  hasExecuted(): boolean {
    return this.executed;
  }

  hasError(): boolean {
    return this.error !== null;
  }

  getError(): Error | null {
    return this.error;
  }
}

export class ParallelQuerySet {
  private queries: ParallelQuery[] = [];
  private maxBatchSize: number;
  private maxConcurrentBatches: number;

  constructor(options: ParallelQueryOptions = {}) {
    this.maxBatchSize = options.maxBatchSize ?? 10;
    this.maxConcurrentBatches = options.maxConcurrentBatches ?? 5;
  }

  add(query: QueryCommand[], blobs: Buffer[] = [], options: ParallelQueryOptions = {}): void {
    this.queries.push(new ParallelQuery(query, blobs, options));
  }

  async execute(client: QueryExecutor): Promise<[any[], Buffer[][]]> {
    const responses: any[] = [];
    const allBlobs: Buffer[][] = [];

    // Process queries in batches
    for (let i = 0; i < this.queries.length; i += this.maxBatchSize) {
      const batch = this.queries.slice(i, i + this.maxBatchSize);
      
      try {
        // Execute batch queries concurrently with Promise.all
        const results = await Promise.all(batch.map(async query => {
          try {
            return await query.execute(client);
          } catch (error) {
            // Wrap the error to preserve the original error message
            throw new Error(`Query execution failed: ${error instanceof Error ? error.message : 'Unknown error'}`);
          }
        }));
        
        // Collect responses and blobs
        results.forEach(([response, blobs]) => {
          responses.push(response);
          allBlobs.push(blobs);
        });
      } catch (error) {
        // If any query in the batch fails, throw the error
        throw error instanceof Error ? error : new Error('Unknown error during batch execution');
      }
    }

    return [responses, allBlobs];
  }

  getQueries(): ParallelQuery[] {
    return this.queries;
  }

  size(): number {
    return this.queries.length;
  }

  clear(): void {
    this.queries = [];
  }
}

export class Parallelizer {
  private client: QueryExecutor;
  private defaultOptions: ParallelQueryOptions;
  private isExecuting: boolean = false;

  constructor(client: QueryExecutor, options: ParallelQueryOptions = {}) {
    this.client = client;
    this.defaultOptions = {
      maxBatchSize: 10,
      maxConcurrentBatches: 5,
      retryAttempts: 3,
      retryDelayMs: 1000,
      ...options
    };
  }

  createQuerySet(options: ParallelQueryOptions = {}): ParallelQuerySet {
    return new ParallelQuerySet({ ...this.defaultOptions, ...options });
  }

  async executeQueries(queries: QueryCommand[][], blobArrays: Buffer[][] = []): Promise<[any[], Buffer[][]]> {
    if (this.isExecuting) {
      throw new Error('Another parallel query execution is in progress');
    }
    
    this.isExecuting = true;
    try {
      const querySet = this.createQuerySet();
      queries.forEach((query, index) => {
        querySet.add(query, blobArrays[index] || [], {
          retryAttempts: this.defaultOptions.retryAttempts,
          retryDelayMs: this.defaultOptions.retryDelayMs
        });
      });
      return await querySet.execute(this.client);
    } finally {
      this.isExecuting = false;
    }
  }

  async executeQueryBatch(queries: ParallelQuery[]): Promise<[any[], Buffer[][]]> {
    if (this.isExecuting) {
      throw new Error('Another parallel query execution is in progress');
    }

    this.isExecuting = true;
    try {
      const responses: any[] = [];
      const allBlobs: Buffer[][] = [];

      const batchSize = this.defaultOptions.maxBatchSize ?? 10;
      const maxConcurrent = this.defaultOptions.maxConcurrentBatches ?? 5;

      // Process queries in batches
      for (let i = 0; i < queries.length; i += batchSize * maxConcurrent) {
        const batchPromises: Promise<[any, Buffer[]]>[] = [];

        // Create batch of concurrent queries
        for (let j = 0; j < maxConcurrent && i + j * batchSize < queries.length; j++) {
          const start = i + j * batchSize;
          const end = Math.min(start + batchSize, queries.length);
          const batch = queries.slice(start, end);

          // Execute each query in the batch with error handling
          batchPromises.push(...batch.map(async query => {
            try {
              return await query.execute(this.client);
            } catch (error) {
              throw new Error(`Query execution failed: ${error instanceof Error ? error.message : 'Unknown error'}`);
            }
          }));
        }

        try {
          // Execute batch queries concurrently
          const results = await Promise.all(batchPromises);
          results.forEach(([response, blobs]) => {
            responses.push(response);
            allBlobs.push(blobs);
          });
        } catch (error) {
          throw error instanceof Error ? error : new Error('Unknown error during batch execution');
        }
      }

      return [responses, allBlobs];
    } finally {
      this.isExecuting = false;
    }
  }
} 