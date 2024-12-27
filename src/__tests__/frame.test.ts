import 'dotenv/config';
import { ApertureClient } from '../client';
import type { FrameMetadata, VideoMetadata, ApertureConfig } from '../types';
import * as fs from 'fs';
import * as path from 'path';

describe('Frame Operations', () => {
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

  describe('Frame CRUD operations', () => {
    const testVideoPath = 'testdata/test.mp4';
    const testVideoProperties = {
      name: 'test-video-for-frames',
      description: 'A test video for frame operations',
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
          name: ['==', testVideoProperties.name]
        }
      });
      
      for (const video of existingVideos) {
        if (video._uniqueid) {
          await client.videos.deleteVideo({ _uniqueid: ["==", video._uniqueid] });
        }
      }

      // Create a test video for frames
      const videoBuffer = fs.readFileSync(testVideoPath);
      await client.videos.addVideo({
        blob: videoBuffer,
        properties: testVideoProperties
      });

      // Find the video to get its reference
      const [video] = await client.videos.findVideos({ 
        constraints: { name: ['==', testVideoProperties.name] }
      });
      expect(video).toBeDefined();
      expect(video._uniqueid).toBeDefined();
      testVideoId = video._uniqueid!;
    });

    describe('Frame creation', () => {
      test('should create a frame using frame_number', async () => {
        const frameProperties = {
          label: 'test-frame-by-number',
          description: 'A test frame specified by frame number',
          scene: 'main character image'
        };

        const frame = await client.frames.addFrame({
          video_ref: testVideoId,
          frame_number: 30, // 1 second at 30fps
          properties: frameProperties
        });

      });

      test('should create a frame using time_offset', async () => {
        const frameProperties = {
          label: 'test-frame-by-time',
          description: 'A test frame specified by time offset',
          scene: 'mid-scene'
        };

        const frame = await client.frames.addFrame({
          video_ref: testVideoId,
          time_offset: '00:00:01.000', // 1 second into video
          properties: frameProperties
        });

        expect(frame).toBeDefined();
        expect(frame.video_ref).toBe(testVideoId);
        expect(frame.time_offset).toBe('00:00:01.000');
        expect(frame.label).toBe(frameProperties.label);
        expect(frame.description).toBe(frameProperties.description);
        expect(frame.scene).toBe(frameProperties.scene);
        expect(frame.created_at).toBeDefined();
        expect(frame.updated_at).toBeDefined();
      });

      test('should create a frame using time_fraction', async () => {
        const frameProperties = {
          label: 'test-frame-by-fraction',
          description: 'A test frame specified by time fraction',
          scene: 'mid-movie frame'
        };

        const frame = await client.frames.addFrame({
          video_ref: testVideoId,
          time_fraction: 0.5, // Middle of the video
          properties: frameProperties
        });

        expect(frame).toBeDefined();
        expect(frame.video_ref).toBe(testVideoId);
        expect(frame.time_fraction).toBe(0.5);
        expect(frame.label).toBe(frameProperties.label);
        expect(frame.description).toBe(frameProperties.description);
        expect(frame.scene).toBe(frameProperties.scene);
        expect(frame.created_at).toBeDefined();
        expect(frame.updated_at).toBeDefined();
      });

      test('should fail to create a frame without specifying position', async () => {
        const frameProperties = {
          label: 'test-frame-invalid',
          description: 'This frame should not be created'
        };

        await expect(client.frames.addFrame({
          video_ref: testVideoId,
          properties: frameProperties
        })).rejects.toThrow('One of frame_number, time_offset, or time_fraction must be provided');
      });

      test('should fail to create a frame with invalid video reference', async () => {
        await expect(client.frames.addFrame({
          video_ref: "invalid-video-ref", // Invalid video reference
          frame_number: 30,
          properties: {
            label: 'test-frame-invalid'
          }
        })).rejects.toThrow();
      });
    });

    describe('Frame retrieval', () => {
      let testFrameId: string;

      beforeAll(async () => {
        // Create a test frame for retrieval tests
        const frame = await client.frames.addFrame({
          video_ref: testVideoId,
          frame_number: 30,
          label: 'test-frame-for-retrieval',
          properties: {
            description: 'A test frame for retrieval tests'
          }
        });

        // Find the newly created frame
        const createdFrame = await client.frames.findFrame({
          constraints: {
            _label: ['==', 'test-frame-for-retrieval']
          }
        });
        testFrameId = createdFrame._uniqueid!;
      });

      test('should find frames by video constraints', async () => {
        const frames = await client.frames.findFramesByVideoConstraints({
          name: ['==', testVideoProperties.name],
          description: ['==', testVideoProperties.description]
        });

        expect(frames).toBeInstanceOf(Array);
        expect(frames.length).toBeGreaterThan(0);
        expect(frames.some(frame => frame._uniqueid === testFrameId)).toBeTruthy();
      });

      test('should find frames with video results', async () => {
        const frames = await client.frames.findFramesByVideoConstraints(
          { name: ['==', testVideoProperties.name] },
          {
            videoResults: {
              properties: ['name', 'fps']
            }
          }
        );

        expect(frames).toBeInstanceOf(Array);
        expect(frames.length).toBeGreaterThan(0);
        expect(frames.some(frame => frame._uniqueid === testFrameId)).toBeTruthy();
      });

      test('should find frames by video name', async () => {
        const frames = await client.frames.findFramesByVideo(testVideoProperties.name);

        expect(frames).toBeInstanceOf(Array);
        expect(frames.length).toBeGreaterThan(0);
        expect(frames.some(frame => frame._uniqueid === testFrameId)).toBeTruthy();
      });

      test('should return empty array for non-existent video name', async () => {
        const frames = await client.frames.findFramesByVideo('non-existent-video');
        expect(frames).toBeInstanceOf(Array);
        expect(frames.length).toBe(0);
      });
    });

    afterAll(async () => {
      // Clean up test video
      if (testVideoId) {
        try {
          await client.videos.deleteVideo({ _uniqueid: ["==", testVideoId] });
        } catch (error) {
          // Ignore errors during cleanup
        }
      }
    });
  });
}); 