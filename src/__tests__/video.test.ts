import 'dotenv/config';
import { ApertureClient } from '../index.js';
import { LogLevel } from '../utils/logger.js';
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
    client.setLogLevel(LogLevel.INFO);
  });

  afterAll(async () => {
    // Ensure proper cleanup of client and connections
    if (client) {
      await client.destroy();
    }
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
    let originalVideoBuffer: Buffer;

    beforeAll(async () => {
      // Ensure test video exists
      expect(fs.existsSync(testVideoPath)).toBeTruthy();
      originalVideoBuffer = fs.readFileSync(testVideoPath);

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

      // Create test video for blob tests
      const video = await client.videos.addVideo({
        blob: originalVideoBuffer,
        properties: testVideoProperties
      });

      // Get video ID for tests
      const videos = await client.videos.findVideos({ 
        constraints: { name: ['==', testVideoProperties.name] }
      });
      expect(videos.length).toBe(1);
      testVideoId = videos[0]._uniqueid!;
    });

    // Add wait between each test
    beforeEach(async () => {
      // Wait for 500ms between tests to ensure operations are complete
      await new Promise(resolve => setTimeout(resolve, 500));
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
          name: 'test-video-blob-2',
          description: 'A second test video'
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

    describe('Blob handling', () => {
      test('findVideo should return video with blob when requested', async () => {
        const video = await client.videos.findVideo({ 
          constraints: { _uniqueid: ['==', testVideoId] },
          blobs: true
        });

        expect(video).toBeDefined();
        expect(video._uniqueid).toBe(testVideoId);
        expect(video._blob).toBeDefined();
        expect(Buffer.isBuffer(video._blob)).toBe(true);
        expect(video._blob!.length).toBeGreaterThan(0);
        
        // Verify the blob matches original video
        expect(video._blob!.equals(originalVideoBuffer)).toBe(true);
        
        // Verify core properties match
        expect(video.name).toBe(testVideoProperties.name);
        expect(video.description).toBe(testVideoProperties.description);
      });

      test('findVideos should return videos with blobs when requested', async () => {
        const videos = await client.videos.findVideos({ 
          constraints: { _uniqueid: ['==', testVideoId] },
          blobs: true
        });

        expect(videos).toHaveLength(1);
        const video = videos[0];
        expect(video._uniqueid).toBe(testVideoId);
        expect(video._blob).toBeDefined();
        expect(Buffer.isBuffer(video._blob)).toBe(true);
        expect(video._blob!.length).toBeGreaterThan(0);
        
        // Verify the blob matches original video
        expect(video._blob!.equals(originalVideoBuffer)).toBe(true);
      });

      test('findVideo should not return blob when not requested', async () => {
        const video = await client.videos.findVideo({ 
          constraints: { _uniqueid: ['==', testVideoId] },
          blobs: false
        });

        expect(video).toBeDefined();
        expect(video._uniqueid).toBe(testVideoId);
        expect(video._blob).toBeUndefined();
      });

      test('findVideos should not return blobs when not requested', async () => {
        const videos = await client.videos.findVideos({ 
          constraints: { _uniqueid: ['==', testVideoId] },
          blobs: false
        });

        expect(videos).toHaveLength(1);
        const video = videos[0];
        expect(video._uniqueid).toBe(testVideoId);
        expect(video._blob).toBeUndefined();
      });
    });

    test('should delete video', async () => {
      await client.videos.deleteVideo({ _uniqueid: ["==", testVideoId] });

      const videos = await client.videos.findVideos({
        constraints: { _uniqueid: ['==', testVideoId] }
      });
      expect(videos.length).toBe(0);
      
      // Clear testVideoId since we've deleted it
      testVideoId = '';
    });

    afterAll(async () => {
      if (testVideoId) {
        // Only cleanup if the deletion test didn't run or failed
        try {
          await client.videos.deleteVideo({ _uniqueid: ["==", testVideoId] });
        } catch (error) {
          // Ignore errors during cleanup
        }
      }
    });
  });
}); 