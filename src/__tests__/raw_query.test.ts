import 'dotenv/config';
import { ApertureClient } from '../index.js';
import type { ApertureConfig } from '../types.js';
import fs from 'fs';
import { LogLevel } from '../utils/logger.js';

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
    client.setLogLevel(LogLevel.TRACE);
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
    const testVideoPath = 'testdata/test.mp4';
    const testVideoProperties = {
      name: 'test-video-raw-query',
      description: 'A test video for raw query',
      fps: 30,
      duration: 10000000 // 10 seconds in microseconds
    };

    let testVideoId: string;
    let originalVideoBuffer: Buffer;

    beforeAll(async () => {
      // Ensure test video exists
      expect(fs.existsSync(testVideoPath)).toBeTruthy();
      originalVideoBuffer = fs.readFileSync(testVideoPath);

      // Create test video using raw query
      const createQuery = [{
        "AddVideo": {
          "properties": testVideoProperties,
          "_ref": 1
        }
      }];

      const [createResponse] = await client.rawQuery(createQuery, [originalVideoBuffer]);
      expect(createResponse[0].AddVideo.status).toBe(0);
      
      // Get video ID using raw query
      const findQuery = [{
        "FindVideo": {
          "constraints": { 
            "name": ["==", testVideoProperties.name]
          },
          "results": {
            "all_properties": true
          }
        }
      }];

      const [findResponse] = await client.rawQuery(findQuery);
      expect(findResponse[0].FindVideo.entities.length).toBe(1);
      testVideoId = findResponse[0].FindVideo.entities[0]._uniqueid;
    });

    test('should handle video query with blob', async () => {
      const query = [{
        "FindVideo": {
          "constraints": { 
            "_uniqueid": ["==", testVideoId]
          },
          "blobs": true,
          "results": {
            "all_properties": true
          }
        }
      }];

      const [response, blobs] = await client.rawQuery(query);
      expect(response).toBeDefined();
      expect(Array.isArray(response)).toBe(true);
      expect(response[0].FindVideo.entities).toBeDefined();
      expect(response[0].FindVideo.entities.length).toBe(1);
      
      const video = response[0].FindVideo.entities[0];
      expect(video.name).toBe(testVideoProperties.name);
      expect(video.description).toBe(testVideoProperties.description);
      
      expect(blobs).toBeDefined();
      expect(Array.isArray(blobs)).toBe(true);
      expect(blobs.length).toBe(1);
      expect(Buffer.isBuffer(blobs[0])).toBe(true);
      expect(blobs[0].equals(originalVideoBuffer)).toBe(true);
    });

    test('should find video by specific ID with blobs', async () => {
      const query = [{
        "FindVideo": {
          "constraints": {
            "id": ["==", "4a08e598-65b6-47cc-9270-18f3375f862d"]
          },
          "results": {
            "all_properties": true
          },
          "blobs": true
        }
      }];

      const [response, blobs] = await client.rawQuery(query);
      expect(response).toBeDefined();
      expect(Array.isArray(response)).toBe(true);
      expect(response[0].FindVideo).toBeDefined();
      expect(response[0].FindVideo.status).toBe(0);
      
      if (response[0].FindVideo.entities?.length > 0) {
        const video = response[0].FindVideo.entities[0];
        expect(video.id).toBe("4a08e598-65b6-47cc-9270-18f3375f862d");
        expect(video._blob_index).toBeDefined();
        expect(blobs[video._blob_index]).toBeDefined();
        expect(Buffer.isBuffer(blobs[video._blob_index])).toBe(true);
      }
    });

    afterAll(async () => {
      if (testVideoId) {
        // Delete video using raw query
        const deleteQuery = [{
          "DeleteVideo": {
            "constraints": {
              "_uniqueid": ["==", testVideoId]
            }
          }
        }];
        
        try {
          await client.rawQuery(deleteQuery);
        } catch (error) {
          // Ignore errors during cleanup
        }
      }
    });

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