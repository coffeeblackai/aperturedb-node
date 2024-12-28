import 'dotenv/config';
import { ApertureClient } from '../client.js';
import type { ApertureConfig } from '../types.js';

describe('Entity Operations', () => {
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

  afterAll(async () => {
    // Ensure proper cleanup of client and connections
    if (client) {
      await client.destroy();
    }
  });

  describe('Entity CRUD operations', () => {
    const testEntity = {
      name: 'test-dataset',
      description: 'A test dataset'
    };

    let testEntityId: string;

    beforeAll(async () => {
      // Clean up any existing test entities
      await client.entities.deleteEntity({
        class: 'dataset',
        constraints: { name: ['==', testEntity.name] }
      });
    });

    test('should create a new entity', async () => {
      const entity = await client.entities.addEntity('dataset', testEntity);
    });

    test('should find entity by name', async () => {
      // Add a small delay to ensure the entity is indexed
      await new Promise(resolve => setTimeout(resolve, 100));
      
      const entities = await client.entities.findEntities({ 
        with_class: 'dataset',
        constraints: { name: ['==', testEntity.name] }
      });
      expect(entities.length).toBe(1);
      expect(entities[0].name).toBe(testEntity.name);
      testEntityId = entities[0]._uniqueid!;
    });

    test('should find entity by multiple constraints', async () => {
      const entities = await client.entities.findEntities({ 
        with_class: 'dataset',
        constraints: { 
          name: ['==', testEntity.name],
          description: ['==', testEntity.description]
        } 
      });
      expect(entities.length).toBe(1);
      expect(entities[0].name).toBe(testEntity.name);
      expect(entities[0].description).toBe(testEntity.description);
    });

    test('should update entity description', async () => {
      const updatedDescription = 'An updated test dataset';
      await client.entities.updateEntity({
        with_class: 'dataset',
        properties: { description: updatedDescription },
        constraints: { _uniqueid: ['==', testEntityId] }
      });

      const entities = await client.entities.findEntities({
        constraints: { _uniqueid: ['==', testEntityId] }
      });
      expect(entities.length).toBe(1);
      expect(entities[0].description).toBe(updatedDescription);
    });

    test('should update multiple properties at once', async () => {
      const updates = {
        name: 'updated-test-dataset',
        description: 'Multiple properties updated'
      };
      
      await client.entities.updateEntity({
        with_class: 'dataset',
        properties: updates,
        constraints: { _uniqueid: ['==', testEntityId] }
      });

      const entities = await client.entities.findEntities({
        constraints: { _uniqueid: ['==', testEntityId] }
      });
      expect(entities.length).toBe(1);
      expect(entities[0].name).toBe(updates.name);
      expect(entities[0].description).toBe(updates.description);
    });

    test('should return empty array for non-existent entity', async () => {
      const entities = await client.entities.findEntities({
        with_class: 'dataset',
        constraints: { name: ['==', 'non-existent-entity'] }
      });
      expect(entities.length).toBe(0);
    });

    test('should delete entity', async () => {
      await client.entities.deleteEntity({
        class: 'dataset',
        constraints: { _uniqueid: ['==', testEntityId] }
      });

      const entities = await client.entities.findEntities({
        constraints: { _uniqueid: ['==', testEntityId] }
      });
      expect(entities.length).toBe(0);
    });

    afterAll(async () => {
      if (testEntityId) {
        // Cleanup in case any test failed
        await client.entities.deleteEntity({
          class: 'dataset',
          constraints: { _uniqueid: ['==', testEntityId] }
        });
      }
    });
  });
}); 