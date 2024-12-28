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
        
        // More comprehensive response validation
        if (!this.response) {
          throw new Error('Empty response received from server');
        }
        
        if (Array.isArray(this.response)) {
          // Check each response in array
          for (const resp of this.response) {
            if (resp && typeof resp === 'object') {
              const command = Object.keys(resp)[0];
              const status = resp[command]?.status;
              if (typeof status === 'number' && status < 0) {
                throw new Error(`${command} failed with status ${status}: ${resp[command]?.info || 'Unknown error'}`);
              }
            }
          }
        } else if (typeof this.response === 'object') {
          const command = Object.keys(this.response)[0];
          const status = this.response[command]?.status;
          if (typeof status === 'number' && status < 0) {
            throw new Error(`${command} failed with status ${status}: ${this.response[command]?.info || 'Unknown error'}`);
          }
        }
        
        this.executed = true;
        return [this.response, this.responseBlobs];
      } catch (error) {
        lastError = error instanceof Error ? error : new Error('Unknown error during query execution');
        Logger.debug(`Query attempt ${attempt + 1}/${this.retryAttempts + 1} failed:`, lastError);
        
        if (attempt < this.retryAttempts) {
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
  private completedResponses: any[] = [];

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
    this.completedResponses = [];
    let batchErrors: Error[] = [];

    // Process queries in batches
    for (let i = 0; i < this.queries.length; i += this.maxBatchSize) {
      // Add small delay between batches to prevent overwhelming the server
      if (i > 0) {
        await new Promise(resolve => setTimeout(resolve, 100));
      }

      const batch = this.queries.slice(i, i + this.maxBatchSize);
      
      // Execute batch queries concurrently with Promise.all
      const batchPromises = batch.map(async query => {
        try {
          const result = await query.execute(client);
          // Store completed response immediately
          this.completedResponses.push(result[0]);
          return { success: true as const, result };
        } catch (error) {
          // Store the error but don't throw yet
          const wrappedError = error instanceof Error ? error : new Error('Unknown error during query execution');
          batchErrors.push(wrappedError);
          Logger.debug('Query in batch failed:', wrappedError);
          return { success: false as const, error: wrappedError };
        }
      });

      const results = await Promise.all(batchPromises);
      
      // Track batch success rate
      let batchSuccessCount = 0;
      
      // Collect successful responses and blobs
      results.forEach(result => {
        if (result.success) {
          const [response, blobs] = result.result;
          responses.push(response);
          allBlobs.push(blobs);
          batchSuccessCount++;
        }
      });

      // If this batch had a high failure rate, add a small delay before next batch
      if (batchSuccessCount < batch.length * 0.5) {
        await new Promise(resolve => setTimeout(resolve, 200));
      }
    }

    // If there were any errors but some successes, log warning
    if (batchErrors.length > 0 && responses.length > 0) {
      Logger.warn(`Parallel query completed with ${batchErrors.length} errors and ${responses.length} successes`);
    }
    
    // If there were only errors, throw the first one
    if (batchErrors.length > 0 && responses.length === 0) {
      const errorMessage = batchErrors.map(e => e.message).join('; ');
      throw new Error(`Query execution failed: ${errorMessage}`);
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
    this.completedResponses = [];
  }

  // Add method to get completed responses
  getCompletedResponses(): any[] {
    return this.completedResponses;
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