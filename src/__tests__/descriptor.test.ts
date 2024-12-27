import 'dotenv/config';
import { ApertureClient } from '../client.js';
import type { ApertureConfig } from '../types.js';

describe('Descriptor Operations', () => {
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

  describe('Descriptor CRUD operations', () => {
    const testDescriptorSet = {
      name: 'test-descriptor-set',
      dimensions: 512,
      metric: 'L2' as const,
      engine: 'Flat' as const
    };

    const testDescriptor = {
      label: 'test-descriptor',
      properties: {
        description: 'A test descriptor',
        category: 'test'
      },
      // Create a random 512-dimensional vector for testing
      blob: new Float32Array(Array(512).fill(0).map(() => Math.random()))
    };

    beforeAll(async () => {
      // Clean up any existing test descriptor sets
      try {
        await client.descriptorSets.deleteDescriptorSet({
          constraints: { name: ['==', testDescriptorSet.name] }
        });
      } catch (error) {
        // Ignore errors if the set doesn't exist
      }

      // Create a test descriptor set
      await client.descriptorSets.addDescriptorSet({
        name: testDescriptorSet.name,
        dimensions: testDescriptorSet.dimensions,
        metric: testDescriptorSet.metric,
        engine: testDescriptorSet.engine
      });

      // Add a small delay to ensure the descriptor set is created
      await new Promise(resolve => setTimeout(resolve, 100));
    });

    describe('Descriptor creation and retrieval', () => {
      test('should create a descriptor', async () => {
        const descriptor = await client.descriptors.addDescriptor({
          set: testDescriptorSet.name,
          blob: testDescriptor.blob,
          label: testDescriptor.label,
          properties: testDescriptor.properties
        });

        expect(descriptor).toBeDefined();
        expect(descriptor.label).toBe(testDescriptor.label);
        expect(descriptor.properties).toEqual(expect.objectContaining(testDescriptor.properties));
        expect(descriptor.created_at).toBeDefined();
        expect(descriptor.updated_at).toBeDefined();
      });

      test('should find descriptor by label', async () => {
        // Add a small delay to ensure the descriptor is indexed
        await new Promise(resolve => setTimeout(resolve, 100));
        
        const descriptors = await client.descriptors.findDescriptors(undefined, {
          set: testDescriptorSet.name,
          labels: true,
          constraints: { _label: ['==', testDescriptor.label] },
          results: { all_properties: true }
        });

        expect(descriptors.length).toBe(1);
        expect(descriptors[0]._label).toBe(testDescriptor.label);
      });

      test('should find similar descriptors', async () => {
        const similarDescriptors = await client.descriptors.findDescriptors(
          testDescriptor.blob,
          {
            set: testDescriptorSet.name,
            k_neighbors: 1,
            distances: true,
            labels: true
          }
        );

        expect(similarDescriptors.length).toBe(1);
        expect(similarDescriptors[0]._label).toBe(testDescriptor.label);
        expect(similarDescriptors[0]._distance).toBeDefined();
      });

      test('should return empty array for non-existent descriptor', async () => {
        const descriptors = await client.descriptors.findDescriptors(undefined, {
          set: testDescriptorSet.name,
          constraints: { label: ['==', 'non-existent-descriptor'] }
        });

        expect(descriptors.length).toBe(0);
      });

      test('should delete descriptor', async () => {
        await client.descriptors.deleteDescriptor({
          set: testDescriptorSet.name,
          constraints: { label: ['==', testDescriptor.label] }
        });

        const descriptors = await client.descriptors.findDescriptors(undefined, {
          set: testDescriptorSet.name,
          constraints: { label: ['==', testDescriptor.label] }
        });

        expect(descriptors.length).toBe(0);
      });
    });

    afterAll(async () => {
      // Clean up the test descriptor set
      try {
        await client.descriptorSets.deleteDescriptorSet({
          with_name: testDescriptorSet.name
        });
      } catch (error) {
        // Ignore cleanup errors
      }
    });
  });
}); 