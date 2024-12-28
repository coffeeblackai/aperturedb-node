import 'dotenv/config';
import { ParallelQuery, ParallelQuerySet, Parallelizer } from '../parallel.js';
import { getTestClient, destroyTestClient } from './helpers/testClient.js';
import { LogLevel } from '../utils/logger.js';

describe('Parallel Query Operations', () => {
  const client = getTestClient();
  const parallelizer = new Parallelizer(client);
  
  beforeAll(() => {
    client.setLogLevel(LogLevel.TRACE);
  });

  afterAll(async () => {
    await destroyTestClient();
  }, 15000);

  describe('ParallelQuery', () => {
    test('should execute a single query', async () => {
      const query = [{
        "FindEntity": {
          "with_class": "test",
          "results": {
            "all_properties": true
          }
        }
      }];

      const parallelQuery = new ParallelQuery(query);
      const [response, blobs] = await parallelQuery.execute(client);
      
      expect(response).toBeDefined();
      expect(Array.isArray(response)).toBe(true);
      expect(blobs).toBeDefined();
      expect(Array.isArray(blobs)).toBe(true);
      expect(parallelQuery.hasExecuted()).toBe(true);
      expect(parallelQuery.hasError()).toBe(false);
    });

    test('should handle query with retry on failure', async () => {
      const invalidQuery = [{
        "InvalidCommand": {}
      }];

      const parallelQuery = new ParallelQuery(invalidQuery, [], {
        retryAttempts: 2,
        retryDelayMs: 100
      });

      await expect(parallelQuery.execute(client)).rejects.toThrow();
      expect(parallelQuery.hasError()).toBe(true);
      expect(parallelQuery.getError()).toBeDefined();
    });
  });

  describe('ParallelQuerySet', () => {
    let querySet: ParallelQuerySet;

    beforeEach(() => {
      querySet = new ParallelQuerySet({ maxBatchSize: 2 });
    });

    afterEach(() => {
      querySet.clear();
    });

    test('should execute multiple queries in parallel', async () => {
      // Add multiple queries
      const queries = [
        [{
          "FindEntity": {
            "with_class": "test1",
            "results": { "all_properties": true }
          }
        }],
        [{
          "FindEntity": {
            "with_class": "test2",
            "results": { "all_properties": true }
          }
        }],
        [{
          "FindEntity": {
            "with_class": "test3",
            "results": { "all_properties": true }
          }
        }]
      ];

      queries.forEach(query => querySet.add(query));
      expect(querySet.size()).toBe(3);

      const [responses, allBlobs] = await querySet.execute(client);
      
      expect(responses).toBeDefined();
      expect(Array.isArray(responses)).toBe(true);
      expect(responses.length).toBe(3);
      expect(allBlobs).toBeDefined();
      expect(Array.isArray(allBlobs)).toBe(true);
      expect(allBlobs.length).toBe(3);
    }, 15000);

    test('should handle mixed success and failure queries', async () => {
      // Add valid and invalid queries
      querySet.add([{
        "FindEntity": {
          "with_class": "test",
          "results": { "all_properties": true }
        }
      }]);

      querySet.add([{
        "InvalidCommand": {}
      }]);

      try {
        await querySet.execute(client);
        fail('Expected query execution to throw an error');
      } catch (error) {
        expect(error).toBeDefined();
        expect(error instanceof Error).toBe(true);
      }
    }, 15000);
  });

  describe('Parallelizer', () => {
    test('should execute query batch', async () => {
      const queries = [
        [{
          "FindEntity": {
            "with_class": "test1",
            "results": { "all_properties": true }
          }
        }],
        [{
          "FindEntity": {
            "with_class": "test2",
            "results": { "all_properties": true }
          }
        }]
      ];

      const [responses, allBlobs] = await parallelizer.executeQueries(queries);
      
      expect(responses).toBeDefined();
      expect(Array.isArray(responses)).toBe(true);
      expect(responses.length).toBe(2);
      expect(allBlobs).toBeDefined();
      expect(Array.isArray(allBlobs)).toBe(true);
      expect(allBlobs.length).toBe(2);
    }, 30000);

    test('should handle query batch with blobs', async () => {
      const queries = [
        [{
          "FindDescriptor": {
            "set": "test_set",
            "k_neighbors": 5,
            "results": { "all_properties": true }
          }
        }],
        [{
          "FindDescriptor": {
            "set": "test_set",
            "k_neighbors": 5,
            "results": { "all_properties": true }
          }
        }]
      ];

      const blobs = [
        [Buffer.from([1, 2, 3, 4])],
        [Buffer.from([5, 6, 7, 8])]
      ];

      const [responses, allBlobs] = await parallelizer.executeQueries(queries, blobs);
      
      expect(responses).toBeDefined();
      expect(Array.isArray(responses)).toBe(true);
      expect(responses.length).toBe(2);
      expect(allBlobs).toBeDefined();
      expect(Array.isArray(allBlobs)).toBe(true);
      expect(allBlobs.length).toBe(2);
    }, 30000);
  });
}); 