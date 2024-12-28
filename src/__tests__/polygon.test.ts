import 'dotenv/config';
import { ApertureClient } from '../client.js';
import type { ImageMetadata, PolygonMetadata, ApertureConfig } from '../types.js';
import * as fs from 'fs';
import * as path from 'path';

describe('Polygon Operations', () => {
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

  describe('Polygon CRUD operations', () => {
    const testImagePath = 'testdata/test.png';
    const testPolygonProperties = {
      name: 'test-polygon',
      label: 'test-label',
      description: 'A test polygon'
    };

    let testImage: ImageMetadata & { _uniqueid: string };
    let testPolygonId: string;

    beforeAll(async () => {
      // Ensure test image exists
      expect(fs.existsSync(testImagePath)).toBeTruthy();

      // Create a test image to use for polygon tests
      const imageBuffer = fs.readFileSync(testImagePath);
      await client.images.addImage({
        blob: imageBuffer,
        properties: {
          name: 'test-image-for-polygons',
          description: 'Test image for polygon tests'
        }
      });
      const foundImages = await client.images.findImages({
        constraints: {
          'name': ['==', 'test-image-for-polygons']
        }
      });

      if (!foundImages.length || !foundImages[0]._uniqueid) {
        throw new Error('Failed to create test image');
      }

      testImage = foundImages[0] as ImageMetadata & { _uniqueid: string };

      // Clean up any existing test polygons
      const existingPolygons = await client.polygons.findPolygons({ 
        constraints: { 
          'name': ['==', testPolygonProperties.name] 
        }
      });
      
      for (const polygon of existingPolygons) {
        if (polygon._uniqueid) {
          await client.polygons.deletePolygon({ _uniqueid: ["==", polygon._uniqueid] });
        }
      }
    });

    afterAll(async () => {
      // Clean up test image
      if (testImage?._uniqueid) {
        await client.images.deleteImage({ _uniqueid: ["==", testImage._uniqueid] });
      }
    });

    describe('Polygon creation', () => {
      test('should create a new polygon', async () => {
        const points: [number, number][] = [
          [10, 10],
          [20, 10],
          [20, 20],
          [10, 20]
        ];

        const polygon = await client.polygons.addPolygon({
          constraints: { _uniqueid: ["==", testImage._uniqueid] },
          points,
          properties: testPolygonProperties
        });

        // Add a small delay to ensure the polygon is indexed
        await new Promise(resolve => setTimeout(resolve, 100));

        // Look up the created polygon to get its ID
        const foundPolygons = await client.polygons.findPolygons({
          constraints: {
            'name': ['==', testPolygonProperties.name]
          },
          uniqueids: true
        });
        expect(foundPolygons.length).toBe(1);
        if (!foundPolygons[0]._uniqueid) {
          throw new Error('Created polygon does not have a _uniqueid');
        }
        testPolygonId = foundPolygons[0]._uniqueid;
        expect(testPolygonId).toBeDefined();
      });

      test('should fail to create a polygon without points', async () => {
        await expect(client.polygons.addPolygon({
          constraints: { _uniqueid: ["==", testImage._uniqueid] },
          points: [],
          properties: testPolygonProperties
        })).rejects.toThrow('points are required for addPolygon');
      });

      test('should fail to create a polygon without image reference', async () => {
        const points: [number, number][] = [
          [10, 10],
          [20, 10],
          [20, 20],
          [10, 20]
        ];

        await expect(client.polygons.addPolygon({
          constraints: {},
          points,
          properties: testPolygonProperties
        })).rejects.toThrow('constraints are required for addPolygon');
      });

      test('should fail to create a polygon with invalid image reference', async () => {
        const points: [number, number][] = [
          [10, 10],
          [20, 10],
          [20, 20],
          [10, 20]
        ];

        await expect(client.polygons.addPolygon({
          constraints: { _uniqueid: ["==", 'invalid-image-ref'] },
          points,
          properties: testPolygonProperties
        })).rejects.toThrow();
      });
    });

    describe('Polygon querying', () => {
      test('should find polygon by name', async () => {
        // Add a small delay to ensure the polygon is indexed
        await new Promise(resolve => setTimeout(resolve, 100));
        
        const polygons = await client.polygons.findPolygons({ 
          constraints: { 
            'name': ['==', testPolygonProperties.name] 
          },
          uniqueids: true
        });
        expect(polygons.length).toBe(1);
        const polygon = polygons[0];
        expect(polygon.name).toBe(testPolygonProperties.name);
        expect(polygon._uniqueid).toBe(testPolygonId);
      });

      test('should find polygon by multiple constraints', async () => {
        const polygons = await client.polygons.findPolygons({ 
          constraints: { 
            'name': ['==', testPolygonProperties.name],
            'label': ['==', testPolygonProperties.label]
          } 
        });
        expect(polygons.length).toBe(1);
        const polygon = polygons[0];
        expect(polygon.name).toBe(testPolygonProperties.name);
        expect(polygon.label).toBe(testPolygonProperties.label);
      });

      test('should return empty array for non-existent polygon', async () => {
        const polygons = await client.polygons.findPolygons({
          constraints: { 
            'name': ['==', 'non-existent-polygon'] 
          }
        });
        expect(polygons.length).toBe(0);
      });
    });

    describe('Polygon deletion', () => {
      test('should delete polygon', async () => {
        console.log("!!!!!!testPolygonId", testPolygonId);
        await client.polygons.deletePolygon({ _uniqueid: ["==",testPolygonId] });

        const polygons = await client.polygons.findPolygons({
          constraints: { 
            '_uniqueid': ['==', testPolygonId] 
          }
        });
        expect(polygons.length).toBe(0);
      });
    });

    describe('Multiple polygon operations', () => {
      test('should handle multiple polygons in the same image', async () => {
        const polygon1Points: [number, number][] = [
          [90, 90],
          [100, 90],
          [100, 100],
          [90, 100]
        ];

        const polygon2Points: [number, number][] = [
          [110, 110],
          [120, 110],
          [120, 120],
          [110, 120]
        ];

        // Create two polygons
        const polygon1 = await client.polygons.addPolygon({
          constraints: { _uniqueid: ["==", testImage._uniqueid] },
          points: polygon1Points,
          properties: {
            name: 'multi-test-1',
            label: 'multi_test_1'
          }
        });

        const polygon2 = await client.polygons.addPolygon({
          constraints: { _uniqueid: ["==", testImage._uniqueid] },
          points: polygon2Points,
          properties: {
            name: 'multi-test-2',
            label: 'multi_test_2'
          }
        });

        // Find all polygons for the image
        const foundPolygons = await client.polygons.findPolygons({
          constraints: {
            'name': ['in', ['multi-test-1', 'multi-test-2']]
          }
        });

        expect(foundPolygons.length).toBe(2);

        // Clean up both polygons
        await client.polygons.deletePolygon({
          _uniqueid: ["==", polygon1._uniqueid]
        });
        await client.polygons.deletePolygon({
            _uniqueid: ["==", polygon2._uniqueid]
        });
      });
    });

    afterAll(async () => {
      if (testPolygonId) {
        // Cleanup in case any test failed
        try {
          await client.polygons.deletePolygon({ _uniqueid: ["==", testPolygonId] });
        } catch (error) {
          // Ignore errors during cleanup
        }
      }
    });
  });
}); 