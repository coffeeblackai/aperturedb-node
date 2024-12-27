import 'dotenv/config';
import { ApertureClient } from '../client.js';
import type { ImageMetadata, BoundingBoxMetadata, ApertureConfig } from '../types.js';
import * as fs from 'fs';
import * as path from 'path';

describe('BoundingBox Operations', () => {
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

  describe('BoundingBox CRUD operations', () => {
    const testImagePath = 'testdata/test.png';
    const testBoundingBoxProperties = {
      name: 'test-bbox',
      label: 'test-label',
      description: 'A test bounding box'
    };

    let testImage: ImageMetadata & { _uniqueid: string };
    let testBoundingBoxId: string;

    beforeAll(async () => {
      // Ensure test image exists
      expect(fs.existsSync(testImagePath)).toBeTruthy();

      // Create a test image to use for bounding box tests
      const imageBuffer = fs.readFileSync(testImagePath);
      await client.images.addImage({
        blob: imageBuffer,
        properties: {
          name: 'test-image-for-bboxes',
          description: 'Test image for bounding box tests'
        }
      });
      const foundImages = await client.images.findImages({
        constraints: {
          'name': ['==', 'test-image-for-bboxes']
        }
      });

      if (!foundImages.length || !foundImages[0]._uniqueid) {
        throw new Error('Failed to create test image');
      }

      testImage = foundImages[0] as ImageMetadata & { _uniqueid: string };

      // Clean up any existing test bounding boxes
      const existingBoundingBoxes = await client.boundingBoxes.findBoundingBoxes({ 
        constraints: { 
          'name': ['==', testBoundingBoxProperties.name] 
        }
      });
      
      for (const bbox of existingBoundingBoxes) {
        if (bbox._uniqueid) {
          await client.boundingBoxes.deleteBoundingBox({ _uniqueid: ["==", bbox._uniqueid] });
        }
      }
    });

    afterAll(async () => {
      // Clean up test image
      if (testImage?._uniqueid) {
        await client.images.deleteImage({ _uniqueid: ["==", testImage._uniqueid] });
      }
    });

    describe('BoundingBox creation', () => {
      test('should create a new bounding box', async () => {
        const bbox = await client.boundingBoxes.addBoundingBox({
          imageId: testImage._uniqueid,
          x: 10,
          y: 10,
          width: 20,
          height: 20,
          properties: testBoundingBoxProperties
        });

        // Add a small delay to ensure the bounding box is indexed
        await new Promise(resolve => setTimeout(resolve, 100));

        // Look up the created bounding box to get its ID
        const foundBoundingBoxes = await client.boundingBoxes.findBoundingBoxes({
          constraints: {
            'name': ['==', testBoundingBoxProperties.name]
          }
        });
        expect(foundBoundingBoxes.length).toBe(1);
        if (!foundBoundingBoxes[0]._uniqueid) {
          throw new Error('Created bounding box does not have a _uniqueid');
        }
        testBoundingBoxId = foundBoundingBoxes[0]._uniqueid;
        expect(testBoundingBoxId).toBeDefined();
      });

      test('should fail to create a bounding box without dimensions', async () => {
        await expect(client.boundingBoxes.addBoundingBox({
          imageId: testImage._uniqueid,
          x: 10,
          y: 10,
          width: 0,
          height: 0,
          properties: testBoundingBoxProperties
        })).rejects.toThrow('x, y, width, and height are required for addBoundingBox');
      });

      test('should fail to create a bounding box without image reference', async () => {
        await expect(client.boundingBoxes.addBoundingBox({
          imageId: '',
          x: 10,
          y: 10,
          width: 20,
          height: 20,
          properties: testBoundingBoxProperties
        })).rejects.toThrow('imageId is required for addBoundingBox');
      });

      test('should fail to create a bounding box with invalid image reference', async () => {
        await expect(client.boundingBoxes.addBoundingBox({
          imageId: 'invalid-image-ref',
          x: 10,
          y: 10,
          width: 20,
          height: 20,
          properties: testBoundingBoxProperties
        })).rejects.toThrow();
      });
    });

    describe('BoundingBox querying', () => {
      test('should find bounding box by name', async () => {
        // Add a small delay to ensure the bounding box is indexed
        await new Promise(resolve => setTimeout(resolve, 100));
        
        const boundingBoxes = await client.boundingBoxes.findBoundingBoxes({ 
          constraints: { 
            'name': ['==', testBoundingBoxProperties.name] 
          }
        });
        expect(boundingBoxes.length).toBe(1);
        const bbox = boundingBoxes[0];
        expect(bbox.name).toBe(testBoundingBoxProperties.name);
        expect(bbox._uniqueid).toBe(testBoundingBoxId);
      });

      test('should find bounding box by multiple constraints', async () => {
        const boundingBoxes = await client.boundingBoxes.findBoundingBoxes({ 
          constraints: { 
            'name': ['==', testBoundingBoxProperties.name],
            'label': ['==', testBoundingBoxProperties.label]
          } 
        });
        expect(boundingBoxes.length).toBe(1);
        const bbox = boundingBoxes[0];
        expect(bbox.name).toBe(testBoundingBoxProperties.name);
        expect(bbox.label).toBe(testBoundingBoxProperties.label);
      });

      test('should return empty array for non-existent bounding box', async () => {
        const boundingBoxes = await client.boundingBoxes.findBoundingBoxes({
          constraints: { 
            'name': ['==', 'non-existent-bbox'] 
          }
        });
        expect(boundingBoxes.length).toBe(0);
      });
    });

    describe('BoundingBox deletion', () => {
      test('should delete bounding box', async () => {
        await client.boundingBoxes.deleteBoundingBox({ _uniqueid: ["==", testBoundingBoxId] });

        const boundingBoxes = await client.boundingBoxes.findBoundingBoxes({
          constraints: { 
            '_uniqueid': ['==', testBoundingBoxId] 
          }
        });
        expect(boundingBoxes.length).toBe(0);
      });
    });

    describe('Multiple bounding box operations', () => {
      test('should handle multiple bounding boxes in the same image', async () => {
        // Create two bounding boxes
        const bbox1 = await client.boundingBoxes.addBoundingBox({
          imageId: testImage._uniqueid,
          x: 90,
          y: 90,
          width: 10,
          height: 10,
          properties: {
            name: 'multi-test-1',
            label: 'multi_test_1'
          }
        });

        const bbox2 = await client.boundingBoxes.addBoundingBox({
          imageId: testImage._uniqueid,
          x: 110,
          y: 110,
          width: 10,
          height: 10,
          properties: {
            name: 'multi-test-2',
            label: 'multi_test_2'
          }
        });

        // Find all bounding boxes for the image
        const foundBoundingBoxes = await client.boundingBoxes.findBoundingBoxes({
          constraints: {
            'name': ['in', ['multi-test-1', 'multi-test-2']]
          }
        });

        expect(foundBoundingBoxes.length).toBe(2);

        // Clean up both bounding boxes
        await client.boundingBoxes.deleteBoundingBox({ name: ["==", "multi-test-1"] });
        await client.boundingBoxes.deleteBoundingBox({ name: ["==", "multi-test-2"] });
      });
    });

    afterAll(async () => {
      if (testBoundingBoxId) {
        // Cleanup in case any test failed
        try {
          await client.boundingBoxes.deleteBoundingBox({ _uniqueid: ["==", testBoundingBoxId] });
        } catch (error) {
          // Ignore errors during cleanup
        }
      }
    });
  });
}); 