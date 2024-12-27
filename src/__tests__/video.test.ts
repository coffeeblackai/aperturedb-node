import 'dotenv/config';
import { ApertureClient } from '../client.js';
import type { VideoMetadata, ApertureConfig } from '../types.js';
import * as fs from 'fs';
import * as path from 'path';

describe('Video Operations', () => {
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

  describe('Video CRUD operations', () => {
    const testVideoPath = 'testdata/test.mp4';
    const testVideoProperties = {
      name: 'test-video-blob',
      description: 'A test video',
      fps: 30,
      duration: 10000000 // 10 seconds in microseconds
    };

    let testVideoId: string;

    beforeAll(async () => {
      // Ensure test video exists
      expect(fs.existsSync(testVideoPath)).toBeTruthy();

      // Clean up any existing test videos
      const existingVideos = await client.videos.findVideos({ 
        constraints: { 
          _uniqueid: ['!=', ""] 
        }
      }) as VideoMetadata[];
      
      for (const video of existingVideos) {
        if (video._uniqueid) {
          await client.videos.deleteVideo({ _uniqueid: ["==", video._uniqueid] });
        }
      }
    });

    // test('should create a new video', async () => {
    //   const video = await client.videos.addVideo({
    //     url: `file://${path.resolve(testVideoPath)}`,
    //     properties: testVideoProperties
    //   });
    // });

    test('should create a new video with blob', async () => {
      const videoBuffer = fs.readFileSync(testVideoPath);
      const video = await client.videos.addVideo({
        blob: videoBuffer,
        properties: {
          ...testVideoProperties,
          name: 'test-video-blob'
        }
      });
    });

    test('should find video by name', async () => {
      // Add a small delay to ensure the video is indexed
      await new Promise(resolve => setTimeout(resolve, 100));
      
      const videos = await client.videos.findVideos({ 
        constraints: { name: ['==', testVideoProperties.name] }
      }) as VideoMetadata[];
      expect(videos.length).toBe(1);
      const video = videos[0];
      expect(video.name).toBe(testVideoProperties.name);
      if (!video._uniqueid) {
        throw new Error('Video _uniqueid not found');
      }
      testVideoId = video._uniqueid;
    });

    test('should find video by multiple constraints', async () => {
      const videos = await client.videos.findVideos({ 
        constraints: { 
          name: ['==', testVideoProperties.name],
          description: ['==', testVideoProperties.description]
        } 
      }) as VideoMetadata[];
      expect(videos.length).toBe(1);
      const video = videos[0];
      expect(video.name).toBe(testVideoProperties.name);
      expect(video.description).toBe(testVideoProperties.description);
    });

    test('should return empty array for non-existent video', async () => {
      const videos = await client.videos.findVideos({
        constraints: { name: ['==', 'non-existent-video'] }
      });
      expect(videos.length).toBe(0);
    });

    test('should delete video', async () => {
      await client.videos.deleteVideo({ _uniqueid: ["==", testVideoId] });

      const videos = await client.videos.findVideos({
        constraints: { _uniqueid: ['==', testVideoId] }
      });
      expect(videos.length).toBe(0);
    });

    afterAll(async () => {
        console.log('testVideoId', testVideoId);
      if (testVideoId) {
        // Cleanup in case any test failed
        try {
          await client.videos.deleteVideo({ _uniqueid: ["==", testVideoId] });
        } catch (error) {
          // Ignore errors during cleanup
        }
      }
    });
  });
}); 