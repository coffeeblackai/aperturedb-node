import 'dotenv/config';
import { ParallelQuery, ParallelQuerySet, Parallelizer } from '../parallel.js';
import { getTestClient, destroyTestClient } from './helpers/testClient.js';
import type { QueryCommand } from '../query.js';

describe('Parallel Query Operations', () => {
  const client = getTestClient();
  const parallelizer = new Parallelizer(client);
  
  beforeAll(async () => {
    console.log('Setting up test data...');
    
    try {
      // Clean up test entities
      const testClasses = ['test1', 'test2', 'test3'];
      for (const className of testClasses) {
        try {
          await client.rawQuery([{
            "DeleteEntity": {
              "with_class": className,
              "constraints": { "name": ["==", className] }
            }
          }]);
        } catch (error) {
          // Ignore if doesn't exist
        }
      }

      await new Promise(resolve => setTimeout(resolve, 2000));
      
      // Create test entities with proper class creation
      for (const className of testClasses) {
        try {
          // First ensure the class exists
          await client.rawQuery([{
            "CreateClass": {
              "name": className,
              "properties": {
                "name": "string"
              }
            }
          }]);
          
          // Then create the entity
          await client.rawQuery([{
            "AddEntity": {
              "class": className,
              "properties": {
                "name": className
              }
            }
          }]);
        } catch (error) {
          console.error(`Failed to create entity ${className}:`, error);
          throw error;
        }
      }
    } catch (error) {
      console.error('Setup failed:', error);
      throw error;
    }
  }, 30000);

  afterAll(async () => {
    try {
      const testClasses = ['test1', 'test2', 'test3'];
      for (const className of testClasses) {
        try {
          await client.rawQuery([{
            "DeleteEntity": {
              "with_class": className,
              "constraints": { "name": ["==", className] }
            }
          }]);
        } catch (error) {
          console.log(`Failed to clean up ${className}:`, error);
        }
      }
    } catch (error) {
      console.error('Cleanup failed:', error);
    } finally {
      await destroyTestClient();
    }
  });

  describe('ParallelQuerySet', () => {
    test('should execute multiple entity queries in parallel', async () => {
      const querySet = parallelizer.createQuerySet({ maxBatchSize: 2 });
      
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
      const [responses, allBlobs] = await querySet.execute(client);
      
      console.log('Entity responses:', JSON.stringify(responses, null, 2));
      
      expect(responses).toBeDefined();
      expect(responses.length).toBe(3);
      
      // Verify each response has correct data
      const foundNames = new Set(responses.map(response => 
        response[0].FindEntity.entities[0].name
      ));
      expect(foundNames).toEqual(new Set(['test1', 'test2', 'test3']));
    }, 15000);

    test('should handle parallel descriptor operations', async () => {
      // First clean up any existing descriptor set
      try {
        await client.rawQuery([{
          "DeleteDescriptorSet": {
            "with_name": "test_set"
          }
        }]);
        await new Promise(resolve => setTimeout(resolve, 2000));
      } catch (error) {
        // Ignore if doesn't exist
      }

      // First create the descriptor set separately
      await client.rawQuery([{
        "AddDescriptorSet": {
          "name": "test_set",
          "dimensions": 4,
          "metric": "L2",
          "engine": "Flat"
        }
      }]);

      // Wait for descriptor set to be ready
      await new Promise(resolve => setTimeout(resolve, 2000));

      const querySet = parallelizer.createQuerySet({ maxBatchSize: 2 });
      
      // Create two descriptors to add
      const descriptor1 = new Float32Array([0.1, 0.2, 0.3, 0.4]);
      const descriptor2 = new Float32Array([0.5, 0.6, 0.7, 0.8]);

      // Add first descriptor
      querySet.add([{
        "AddDescriptor": {
          "set": "test_set",
          "properties": { "label": "test1" }
        } as any
      }], [Buffer.from(descriptor1.buffer)]);

      // Add second descriptor
      querySet.add([{
        "AddDescriptor": {
          "set": "test_set",
          "properties": { "label": "test2" }
        } as any
      }], [Buffer.from(descriptor2.buffer)]);

      const [responses, blobs] = await querySet.execute(client);
      
      console.log('Descriptor responses:', JSON.stringify(responses, null, 2));
      
      expect(responses).toBeDefined();
      expect(responses.length).toBe(2);
      
      // Verify both descriptors were added - fix response structure access
      expect(responses[0][0].AddDescriptor.status).toBe(0);
      expect(responses[1][0].AddDescriptor.status).toBe(0);

      // Clean up
      await client.rawQuery([{
        "DeleteDescriptorSet": {
          "with_name": "test_set"
        }
      }]);
    }, 15000);
  });
}); 