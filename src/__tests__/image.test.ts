import 'dotenv/config';
import { ApertureClient } from '../client.js';
import type { ImageMetadata, ApertureConfig } from '../types.js';
import * as fs from 'fs';
import * as path from 'path';

describe('Image Operations', () => {
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

  describe('Image CRUD operations', () => {
    const testImagePath = 'testdata/test.png';
    const testImageProperties = {
      name: 'test-image-blob',
      description: 'A test image',
      width: 640,
      height: 480
    };

    let testImageId: string;

    beforeAll(async () => {
      // Ensure test image exists
      expect(fs.existsSync(testImagePath)).toBeTruthy();

      // Clean up any existing test images
      const existingImages = await client.images.findImages({ 
        constraints: { 
          name: ['==', testImageProperties.name] 
        }
      });
      
      for (const image of existingImages) {
        if (image._uniqueid) {
          await client.images.deleteImage({ _uniqueid: ["==", image._uniqueid] });
        }
      }
    });

    test('should create a new image with blob', async () => {
      const imageBuffer = fs.readFileSync(testImagePath);
      const image = await client.images.addImage({
        blob: imageBuffer,
        properties: {
          ...testImageProperties,
          name: 'test-image-blob'
        }
      });
    });

    test('should find image by name', async () => {
      // Add a small delay to ensure the image is indexed
      await new Promise(resolve => setTimeout(resolve, 100));
      
      const images = await client.images.findImages({ 
        constraints: { name: ['==', testImageProperties.name] }
      });
      expect(images.length).toBe(1);
      const image = images[0];
      expect(image.name).toBe(testImageProperties.name);
      if (!image._uniqueid) {
        throw new Error('Image _uniqueid not found');
      }
      testImageId = image._uniqueid;
    });

    test('should find image by multiple constraints', async () => {
      const images = await client.images.findImages({ 
        constraints: { 
          name: ['==', testImageProperties.name],
          description: ['==', testImageProperties.description]
        } 
      });
      expect(images.length).toBe(1);
      const image = images[0];
      expect(image.name).toBe(testImageProperties.name);
      expect(image.description).toBe(testImageProperties.description);
    });

    test('should return empty array for non-existent image', async () => {
      const images = await client.images.findImages({
        constraints: { name: ['==', 'non-existent-image'] }
      });
      expect(images.length).toBe(0);
    });

    test('should delete image', async () => {
      await client.images.deleteImage({ _uniqueid: ["==", testImageId] });

      const images = await client.images.findImages({
        constraints: { _uniqueid: ['==', testImageId] }
      });
      expect(images.length).toBe(0);
    });

    afterAll(async () => {
      if (testImageId) {
        // Cleanup in case any test failed
        try {
          await client.images.deleteImage({ _uniqueid: ["==", testImageId] });
        } catch (error) {
          // Ignore errors during cleanup
        }
      }
    });
  });
}); 