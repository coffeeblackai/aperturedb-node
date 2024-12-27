import 'dotenv/config';
import { ApertureClient } from '../client.js';
import type { ClipMetadata, VideoMetadata, ApertureConfig } from '../types.js';
import * as fs from 'fs';
import * as path from 'path';

describe('Clip Operations', () => {
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

  describe('Clip CRUD operations', () => {
    const testVideoPath = 'testdata/test.mp4';
    const testVideoProperties = {
      name: 'test-video-for-clips',
      description: 'A test video for clip operations',
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

      // Create a test video for clips
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

    describe('Clip creation', () => {
      test('should create a clip using frame_number_range', async () => {
        const clipProperties = {
          label: 'test-clip-by-frames',
          description: 'A test clip specified by frame numbers',
          scene: 'opening scene'
        };

        const clip = await client.clips.addClip({
          video_ref: testVideoId,
          frame_number_range: {
            start: 0,
            stop: 30 // First second at 30fps
          },
          label: clipProperties.label,
          properties: clipProperties
        });

        expect(clip).toBeDefined();
        expect(clip.video_ref).toBe(testVideoId);
        expect(clip.frame_number_range).toEqual({ start: 0, stop: 30 });
        expect(clip.label).toBe(clipProperties.label);
        expect(clip.description).toBe(clipProperties.description);
        expect(clip.scene).toBe(clipProperties.scene);
        expect(clip.created_at).toBeDefined();
        expect(clip.updated_at).toBeDefined();
      });

      test('should create a clip using time_offset_range', async () => {
        const clipProperties = {
          label: 'test-clip-by-time',
          description: 'A test clip specified by time offset',
          scene: 'middle scene'
        };

        const clip = await client.clips.addClip({
          video_ref: testVideoId,
          time_offset_range: {
            start: '00:00:01.000',
            stop: '00:00:02.000' // 1 second clip starting at 1 second
          },
          label: clipProperties.label,
          properties: clipProperties
        });

        expect(clip).toBeDefined();
        expect(clip.video_ref).toBe(testVideoId);
        expect(clip.time_offset_range).toEqual({
          start: '00:00:01.000',
          stop: '00:00:02.000'
        });
        expect(clip.label).toBe(clipProperties.label);
        expect(clip.description).toBe(clipProperties.description);
        expect(clip.scene).toBe(clipProperties.scene);
        expect(clip.created_at).toBeDefined();
        expect(clip.updated_at).toBeDefined();
      });

      test('should create a clip using time_fraction_range', async () => {
        const clipProperties = {
          label: 'test-clip-by-fraction',
          description: 'A test clip specified by time fraction',
          scene: 'ending scene'
        };

        const clip = await client.clips.addClip({
          video_ref: testVideoId,
          time_fraction_range: {
            start: 0.8,
            stop: 1.0 // Last 20% of the video
          },
          label: clipProperties.label,
          properties: clipProperties
        });

        expect(clip).toBeDefined();
        expect(clip.video_ref).toBe(testVideoId);
        expect(clip.time_fraction_range).toEqual({ start: 0.8, stop: 1.0 });
        expect(clip.label).toBe(clipProperties.label);
        expect(clip.description).toBe(clipProperties.description);
        expect(clip.scene).toBe(clipProperties.scene);
        expect(clip.created_at).toBeDefined();
        expect(clip.updated_at).toBeDefined();
      });

      test('should fail to create a clip without specifying range', async () => {
        const clipProperties = {
          label: 'test-clip-invalid',
          description: 'This clip should not be created'
        };

        await expect(client.clips.addClip({
          video_ref: testVideoId,
          properties: clipProperties
        })).rejects.toThrow('One of frame_number_range, time_offset_range, or time_fraction_range must be provided');
      });

      test('should fail to create a clip with invalid video reference', async () => {
        await expect(client.clips.addClip({
          video_ref: "invalid-video-ref",
          frame_number_range: {
            start: 0,
            stop: 30
          },
          properties: {
            label: 'test-clip-invalid'
          }
        })).rejects.toThrow();
      });
    });

    describe('Clip retrieval', () => {
      let testClipId: string;

      beforeAll(async () => {
        // Create a test clip for retrieval tests
        await client.clips.addClip({
          video_ref: testVideoId,
          frame_number_range: {
            start: 0,
            stop: 30
          },
          label: 'test-clip-for-retrieval',
          properties: {
            description: 'A test clip for retrieval tests'
          }
        });

        // Find the newly created clip
        const clip = await client.clips.findClip({
          constraints: {
            _label: ['==', 'test-clip-for-retrieval']
          }
        });
        if (!clip || !clip._uniqueid) {
          throw new Error('Failed to create test clip');
        }
        testClipId = clip._uniqueid;
      });

      test('should find a clip by uniqueid', async () => {
        const clip = await client.clips.findClip({
          constraints: {
            _uniqueid: ['==', testClipId]
          },
          uniqueids: true,
        });

        expect(clip).toBeDefined();
        expect(clip?._uniqueid).toBe(testClipId);
      });

      test('should find clips by video constraints', async () => {
        const clips = await client.clips.findClipsByVideoConstraints({
          name: ['==', testVideoProperties.name],
          description: ['==', testVideoProperties.description]
        });

        expect(clips).toBeInstanceOf(Array);
        expect(clips.length).toBeGreaterThan(0);
        expect(clips.some(clip => clip._uniqueid === testClipId)).toBeTruthy();
      });


      test('should return empty array for non-existent video name', async () => {
        const clips = await client.clips.findClipsByVideo('non-existent-video');
        expect(clips).toBeInstanceOf(Array);
        expect(clips.length).toBe(0);
      });
    });

    describe('Clip updates', () => {
      let testClipId: string;

      beforeAll(async () => {
        // Create a test clip for update tests
        await client.clips.addClip({
          video_ref: testVideoId,
          frame_number_range: {
            start: 0,
            stop: 30
          },
          label: 'test-clip-for-updates',
          properties: {
            description: 'Original description'
          }
        });

        // Find the newly created clip
        const clip = await client.clips.findClip({
          constraints: {
            _label: ['==', 'test-clip-for-updates']
          }
        });
        if (!clip || !clip._uniqueid) {
          throw new Error('Failed to create test clip');
        }
        testClipId = clip._uniqueid;
      });

      test('should update clip properties', async () => {
        await client.clips.updateClip({
          constraints: {
            _uniqueid: ['==', testClipId]
          },
          properties: {
            description: 'Updated description',
            new_property: 'new value'
          }
        });

        const updatedClip = await client.clips.findClip({
          constraints: {
            _uniqueid: ['==', testClipId]
          },
          uniqueids: true
        });

        expect(updatedClip).toBeDefined();
        expect(updatedClip?.description).toBe('Updated description');
        expect(updatedClip?.new_property).toBe('new value');
      });

      test('should remove clip properties', async () => {
        await client.clips.updateClip({
          constraints: {
            _uniqueid: ['==', testClipId]
          },
          remove_props: ['new_property']
        });

        const updatedClip = await client.clips.findClip({
          constraints: {
            _uniqueid: ['==', testClipId]
          },
          uniqueids: true
        });

        expect(updatedClip).toBeDefined();
        expect(updatedClip?.new_property).toBeUndefined();
      });
    });

    describe('Clip deletion', () => {
      let testClipId: string;

      beforeAll(async () => {
        // Create a test clip for deletion tests
        await client.clips.addClip({
          video_ref: testVideoId,
          frame_number_range: {
            start: 0,
            stop: 30
          },
          label: 'test-clip-for-deletion',
          properties: {
            description: 'A test clip for deletion'
          }
        });

        // Find the newly created clip
        const clip = await client.clips.findClip({
          constraints: {
            _label: ['==', 'test-clip-for-deletion']
          },
          uniqueids: true
        });
        if (!clip || !clip._uniqueid) {
          throw new Error('Failed to create test clip');
        }
        testClipId = clip._uniqueid;
      });

      test('should delete a clip', async () => {
        await client.clips.deleteClip({
          _uniqueid: ['==', testClipId]
        });

        const deletedClip = await client.clips.findClip({
          constraints: {
            _uniqueid: ['==', testClipId]
          },
          uniqueids: true
        });
        expect(deletedClip).toBeUndefined();
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