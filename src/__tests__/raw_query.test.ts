import 'dotenv/config';
import { ApertureClient } from '../client.js';
import type { ApertureConfig } from '../types.js';

describe('Raw Query Operations', () => {
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

  describe('Basic Query Operations', () => {
    test('should execute a simple FindEntity query', async () => {
      const query = [{
        "FindEntity": {
          "with_class": "test",
          "results": {
            "all_properties": true
          }
        }
      }];

      const [response, blobs] = await client.rawQuery(query);
      expect(response).toBeDefined();
      expect(Array.isArray(response)).toBe(true);
      expect(blobs).toBeDefined();
      expect(Array.isArray(blobs)).toBe(true);
    });

    test('should handle multiple commands in a single query', async () => {
      const query = [{
        "FindEntity": {
          "with_class": "test",
          "_ref": 1,
          "results": {
            "all_properties": true
          }
        }
      }, {
        "FindEntity": {
          "is_connected_to": {
            "ref": 1
          },
          "results": {
            "all_properties": true
          }
        }
      }];

      const [response, blobs] = await client.rawQuery(query);
      expect(response).toBeDefined();
      expect(Array.isArray(response)).toBe(true);
      expect(response.length).toBe(2);
    });

    test('should handle query with constraints', async () => {
      const query = [{
        "FindEntity": {
          "with_class": "test",
          "constraints": {
            "name": ["==", "test_entity"]
          },
          "results": {
            "all_properties": true
          }
        }
      }];

      const [response, blobs] = await client.rawQuery(query);
      expect(response).toBeDefined();
      expect(Array.isArray(response)).toBe(true);
    });
  });

  describe('Binary Blob Handling', () => {
    test('should handle query with binary blob', async () => {
      const blob = Buffer.from([1, 2, 3, 4]);
      const query = [{
        "FindDescriptor": {
          "set": "test_set",
          "k_neighbors": 5,
          "results": {
            "all_properties": true
          }
        }
      }];

      const [response, blobs] = await client.rawQuery(query, [blob]);
      expect(response).toBeDefined();
    //   expect(Array.isArray(response)).toBe(true);
    });

    test('should handle query with multiple blobs', async () => {
      const blob1 = Buffer.from([1, 2, 3, 4]);
      const blob2 = Buffer.from([5, 6, 7, 8]);
      const query = [{
        "FindDescriptor": {
          "set": "test_set",
          "k_neighbors": 5,
          "results": {
            "all_properties": true
          }
        }
      }];

      const [response, blobs] = await client.rawQuery(query, [blob1, blob2]);
      expect(response).toBeDefined();
    //   expect(Array.isArray(response)).toBe(true);
    });
  });

  describe('Type Safety', () => {
    interface CustomResponse {
      FindEntity: {
        status: number;
        entities: Array<{
          _uniqueid: string;
          name: string;
        }>;
      };
    }

    test('should handle typed response', async () => {
      const query = [{
        "FindEntity": {
          "with_class": "test",
          "results": {
            "all_properties": true
          }
        }
      }];

      const [response] = await client.rawQuery<CustomResponse[]>(query);
      expect(response).toBeDefined();
      if (response[0]?.FindEntity?.entities) {
        const entity = response[0].FindEntity.entities[0];
        expect(typeof entity?._uniqueid).toBe('string');
      }
    });
  });
}); 