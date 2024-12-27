import 'dotenv/config';
import { ApertureClient } from '../client.js';
import type { ApertureConfig, DescriptorSet } from '../types.js';

describe('DescriptorSet Operations', () => {
  let client: ApertureClient;
  
  beforeAll(async () => {
    const config: ApertureConfig = {
      host: process.env.APERTURE_HOST!,
      port: 55555,
      username: process.env.APERTURE_USER!,
      password: process.env.APERTURE_PASSWORD!,
      useSsl: true,
      useKeepalive: true,
      retryIntervalSeconds: 5,
      retryMaxAttempts: 1
    };

    client = ApertureClient.getInstance(config);
  });

  describe('DescriptorSet CRUD operations', () => {
    const testDescriptorSet: Partial<DescriptorSet> = {
      name: 'test-descriptor-set',
      dimensions: 512,
      metric: 'L2',
      engine: 'HNSW'
    };

    beforeAll(async () => {
      // Clean up any existing test descriptor sets
      await client.descriptorSets.deleteDescriptorSet({
        with_name: testDescriptorSet.name
      });
    });

    test('should create a new descriptor set', async () => {
      const descriptorSet = await client.descriptorSets.addDescriptorSet(testDescriptorSet);
      expect(descriptorSet.name).toBe(testDescriptorSet.name);
      expect(descriptorSet.dimensions).toBe(testDescriptorSet.dimensions);
      expect(descriptorSet.metric).toBe(testDescriptorSet.metric);
      expect(descriptorSet.engine).toBe(testDescriptorSet.engine);
      expect(descriptorSet.created_at).toBeDefined();
      expect(descriptorSet.updated_at).toBeDefined();
    });

    test('should find descriptor set by name', async () => {
      // Add a small delay to ensure the descriptor set is indexed
      await new Promise(resolve => setTimeout(resolve, 100));
      
      const descriptorSets = await client.descriptorSets.findDescriptorSet({
        with_name: testDescriptorSet.name,
        engines: true,
        metrics: true,
        dimensions: true,
        results: { all_properties: true }
      });
      
      expect(descriptorSets).toHaveLength(1);
      const found = descriptorSets[0];
      expect(found._name).toBe(testDescriptorSet.name);
      expect(found._dimensions).toBe(testDescriptorSet.dimensions);
      expect(found._uniqueid).toBeDefined();
    });

    test('should find descriptor set with constraints', async () => {
      const descriptorSets = await client.descriptorSets.findDescriptorSet({
        constraints: {
          _name: ['==', testDescriptorSet.name]
        },
        results: { all_properties: true }
      });
      
      expect(descriptorSets).toHaveLength(1);
      expect(descriptorSets[0]._name).toBe(testDescriptorSet.name);
    });

    test('should return empty array when no descriptor sets match', async () => {
      const descriptorSets = await client.descriptorSets.findDescriptorSet({
        with_name: 'non-existent-descriptor-set',
        results: { all_properties: true }
      });
      
      expect(descriptorSets).toHaveLength(0);
    });

    test('should find descriptor set with metrics and engines', async () => {
      const descriptorSets = await client.descriptorSets.findDescriptorSet({
        with_name: testDescriptorSet.name,
        metrics: true,
        engines: true
      });
      
      expect(descriptorSets).toHaveLength(1);
      const found = descriptorSets[0];
      expect(found._name).toBe(testDescriptorSet.name);
      expect(found._metrics).toBeDefined();
      expect(found._metrics).toBeInstanceOf(Array);
      expect(found._engines).toBeDefined();
      expect(found._engines).toBeInstanceOf(Array);
    });

    test('should create descriptor set with default values', async () => {
      const minimalSet = {
        name: 'minimal-descriptor-set'
      };

      const descriptorSet = await client.descriptorSets.addDescriptorSet(minimalSet);
      expect(descriptorSet.name).toBe(minimalSet.name);
      expect(descriptorSet.dimensions).toBe(512); // default value
      expect(descriptorSet.metric).toBe('L2'); // default value
      expect(descriptorSet.engine).toBe('HNSW'); // default value

      // Clean up
      await client.descriptorSets.deleteDescriptorSet({
        with_name: minimalSet.name
      });
    });


    test('should throw error when creating descriptor set without name', async () => {
      await expect(client.descriptorSets.addDescriptorSet({}))
        .rejects
        .toThrow('name is required for addDescriptorSet');
    });

    afterAll(async () => {
      // Cleanup in case any test failed
      await client.descriptorSets.deleteDescriptorSet({
        with_name: testDescriptorSet.name
      });
    });
  });
}); 