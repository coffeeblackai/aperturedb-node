import 'dotenv/config';
import { ApertureClient } from '../client.js';
import type { ApertureConfig } from '../types.js';
import fs from 'fs';

describe('Connection Operations', () => {
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

  describe('Entity-to-Entity Connection', () => {
    const sourceEntity = {
      name: 'John Doe',
      age: 30,
      email: 'john@example.com'
    };

    const targetEntity = {
      name: 'Jane Smith',
      age: 28,
      email: 'jane@example.com'
    };

    const connectionProperties = {
      relationship: 'colleague',
      department: 'Engineering',
      since: '2023-01-01'
    };

    let sourceEntityId: string;
    let targetEntityId: string;
    let connectionId: string;

    beforeAll(async () => {
      // Clean up any existing test entities and their connections
      await client.connections.findAndDeleteConnections({
        with_class: 'WorksWith',
        constraints: {
          relationship: ['==', connectionProperties.relationship],
          department: ['==', connectionProperties.department]
        }
      });

      await client.entities.deleteEntity({
        class: 'Person',
        constraints: { email: ['==', sourceEntity.email] }
      });

      await client.entities.deleteEntity({
        class: 'Person',
        constraints: { email: ['==', targetEntity.email] }
      });
    });

    test('should create two entities and connect them', async () => {
      // Create the connection using addConnectionWithObjects
      await client.connections.addConnectionWithObjects({
        connectionClass: 'WorksWith',
        sourceObject: {
          operation: 'Add',
          type: 'Entity',
          class: 'Person',
          properties: sourceEntity
        },
        targetObject: {
          operation: 'Add',
          type: 'Entity',
          class: 'Person',
          properties: targetEntity
        },
        connectionProperties
      });

      // Find the source entity
      const [foundSourceEntity] = await client.entities.findEntities({
        with_class: 'Person',
        constraints: { name: ['==', sourceEntity.name] }
      });
      expect(foundSourceEntity).toBeDefined();
      sourceEntityId = foundSourceEntity._uniqueid!;

      // Find the target entity
      const [foundTargetEntity] = await client.entities.findEntities({
        with_class: 'Person',
        constraints: { name: ['==', targetEntity.name] },
        results: {
          all_properties: true
        }
      });
      console.log("!!!!!!", foundTargetEntity);
      expect(foundTargetEntity).toBeDefined();
      targetEntityId = foundTargetEntity._uniqueid!;

      // Verify the entities were created correctly
      expect(foundSourceEntity.name).toBe(sourceEntity.name);
      expect(foundSourceEntity.email).toBe(sourceEntity.email);

      expect(foundTargetEntity.name).toBe(targetEntity.name);
      expect(foundTargetEntity.email).toBe(targetEntity.email);

      // Verify we can find the connection
      const connections = await client.connections.findConnections({
        with_class: 'WorksWith',
        constraints: {
          relationship: ['==', connectionProperties.relationship],
          department: ['==', connectionProperties.department]
        }
      });

      expect(connections.length).toBe(1);
      connectionId = connections[0]._uniqueid!;

      expect(connections[0].class).toBe('WorksWith');
    });

    test('should create entity first, then find and connect it to new entity', async () => {
      // First create the source entity
      await client.entities.addEntity('Person', sourceEntity);

      // Find the created entity by name
      const [createdEntity] = await client.entities.findEntities({
        with_class: 'Person',
        constraints: { name: ['==', sourceEntity.name] }
      });

      expect(createdEntity).toBeDefined();
      const createdSourceId = createdEntity._uniqueid!;

      // Now create connection using find operation for source and add for target
      await client.connections.addConnectionWithObjects({
        connectionClass: 'WorksWith',
        sourceObject: {
          operation: 'Find',
          type: 'Entity',
          with_class: 'Person',
          constraints: { _uniqueid: ['==', createdSourceId] }
        },
        targetObject: {
          operation: 'Add',
          type: 'Entity',
          class: 'Person',
          properties: targetEntity
        },
        connectionProperties
      });

      // Find the target entity
      const [foundTargetEntity] = await client.entities.findEntities({
        with_class: 'Person',
        constraints: { name: ['==', targetEntity.name] }
      });
      expect(foundTargetEntity).toBeDefined();

      // Store IDs for cleanup
      sourceEntityId = createdSourceId;
      targetEntityId = foundTargetEntity._uniqueid!;

      expect(foundTargetEntity.name).toBe(targetEntity.name);
      expect(foundTargetEntity.email).toBe(targetEntity.email);

      // Verify we can find the connection
      const connections = await client.connections.findConnections({
        with_class: 'WorksWith',
        constraints: {
          relationship: ['==', connectionProperties.relationship],
          department: ['==', connectionProperties.department]
        }
      });

      expect(connections.length).toBe(2);
      connectionId = connections[0]._uniqueid!;

      // Verify connection properties
      expect(connections[0].class).toBe('WorksWith');
    });

    test('should successfully delete a connection', async () => {
      // Create a new connection for deletion test
      await client.connections.addConnectionWithObjects({
        connectionClass: 'WorksWith',
        sourceObject: {
          operation: 'Add',
          type: 'Entity',
          class: 'Person',
          properties: sourceEntity
        },
        targetObject: {
          operation: 'Add',
          type: 'Entity',
          class: 'Person',
          properties: targetEntity
        },
        connectionProperties
      });

      // Find the connection we just created
      const [connectionToDelete] = await client.connections.findConnections({
        with_class: 'WorksWith',
        constraints: {
          relationship: ['==', connectionProperties.relationship],
          department: ['==', connectionProperties.department]
        }
      });

      expect(connectionToDelete).toBeDefined();
      const deleteConnectionId = connectionToDelete._uniqueid!;

      // Delete the connection
      await client.connections.deleteConnection({
        with_class: 'WorksWith',
        constraints: { _uniqueid: ['==', deleteConnectionId] }
      });

      // Verify the connection no longer exists
      const connectionsAfterDelete = await client.connections.findConnections({
        with_class: 'WorksWith',
        constraints: { _uniqueid: ['==', deleteConnectionId] }
      });

      expect(connectionsAfterDelete.length).toBe(0);
    });

    test('should successfully update connection properties', async () => {
      // Create a new connection for update test
      await client.connections.addConnectionWithObjects({
        connectionClass: 'WorksWith',
        sourceObject: {
          operation: 'Add',
          type: 'Entity',
          class: 'Person',
          properties: sourceEntity
        },
        targetObject: {
          operation: 'Add',
          type: 'Entity',
          class: 'Person',
          properties: targetEntity
        },
        connectionProperties
      });

      // Find the connection we just created
      const [connectionToUpdate] = await client.connections.findConnections({
        with_class: 'WorksWith',
        constraints: {
          relationship: ['==', connectionProperties.relationship],
          department: ['==', connectionProperties.department]
        }
      });

      expect(connectionToUpdate).toBeDefined();
      const updateConnectionId = connectionToUpdate._uniqueid!;

      // Update the connection properties
      const updatedProperties = {
        relationship: 'manager',
        department: 'Executive',
        since: '2024-01-01'
      };

      await client.connections.updateConnection({
        with_class: 'WorksWith',
        constraints: { _uniqueid: ['==', updateConnectionId] },
        properties: updatedProperties
      });

      // Verify the connection was updated
      const [updatedConnection] = await client.connections.findConnections({
        with_class: 'WorksWith',
        constraints: { _uniqueid: ['==', updateConnectionId] }
      });

      expect(updatedConnection).toBeDefined();
      expect(updatedConnection.relationship).toBe(updatedProperties.relationship);
      expect(updatedConnection.department).toBe(updatedProperties.department);
      expect(updatedConnection.since).toBe(updatedProperties.since);
    });

    afterAll(async () => {
      // Clean up in case test failed
      if (connectionId) {
        await client.connections.deleteConnection({
          with_class: 'WorksWith',
          constraints: { _uniqueid: ['==', connectionId] }
        });
      }

      if (sourceEntityId) {
        await client.entities.deleteEntity({
          class: 'Person',
          constraints: { _uniqueid: ['==', sourceEntityId] }
        });
      }

      if (targetEntityId) {
        await client.entities.deleteEntity({
          class: 'Person',
          constraints: { _uniqueid: ['==', targetEntityId] }
        });
      }
    });
  });

  describe('Image-to-Descriptor Connection', () => {
    const testImagePath = 'testdata/test.png';
    const testImageProperties = {
      name: 'test-image-for-descriptor',
      description: 'A test image for descriptor connection'
    };

    const testDescriptorSet = {
      name: 'test-descriptor-set-for-connection',
      dimensions: 512,
      metric: 'L2' as const,
      engine: 'Flat' as const
    };

    const testDescriptor = {
      label: 'test-descriptor-for-connection',
      properties: {
        description: 'A test descriptor for connection testing',
        category: 'test'
      },
      // Create a random 512-dimensional vector for testing
      blob: new Float32Array(Array(512).fill(0).map(() => Math.random()))
    };

    let imageId: string;
    let descriptorId: string;
    let connectionId: string;

    beforeAll(async () => {
      // Clean up any existing test images
      await client.images.findImages({ 
        constraints: { name: ['==', testImageProperties.name] }
      }).then(images => {
        return Promise.all(images.map(image => 
          client.images.deleteImage({ _uniqueid: ["==", image._uniqueid!] })
        ));
      });

      // Clean up any existing test descriptor sets
      try {
        await client.descriptorSets.deleteDescriptorSet({
          with_name: testDescriptorSet.name
        });
      } catch (error) {
        // Ignore errors if the set doesn't exist
      }

      // Clean up any existing test connections
      await client.connections.findAndDeleteConnections({
        with_class: 'DescribesImage',
        constraints: {
          description: ['==', 'Test connection between image and descriptor']
        }
      });
    });

    test('should create image, descriptor, and connect them', async () => {
      // Create the image
      const imageBuffer = fs.readFileSync(testImagePath);
      await client.images.addImage({
        blob: imageBuffer,
        properties: testImageProperties
      });

      // Find the created image
      const [image] = await client.images.findImages({ 
        constraints: { name: ['==', testImageProperties.name] }
      });
      expect(image).toBeDefined();
      expect(image._uniqueid).toBeDefined();
      imageId = image._uniqueid!;

      // Create the descriptor set
      await client.descriptorSets.addDescriptorSet(testDescriptorSet);

      // Add a small delay to ensure the descriptor set is created
      await new Promise(resolve => setTimeout(resolve, 100));

      // Create the descriptor
      const descriptor = await client.descriptors.addDescriptor({
        set: testDescriptorSet.name,
        blob: testDescriptor.blob,
        label: testDescriptor.label,
        properties: testDescriptor.properties
      });

      // Find the created descriptor
      const [foundDescriptor] = await client.descriptors.findDescriptors(undefined, {
        set: testDescriptorSet.name,
        labels: true,
        constraints: { _label: ['==', testDescriptor.label] }
      });
      expect(foundDescriptor).toBeDefined();
      descriptorId = foundDescriptor._uniqueid!;

      // Create the connection using addConnectionWithObjects
      await client.connections.addConnectionWithObjects({
        connectionClass: 'DescribesImage',
        sourceObject: {
          operation: 'Find',
          type: 'Descriptor',
          set: testDescriptorSet.name,
          constraints: { _uniqueid: ['==', descriptorId] }
        },
        targetObject: {
          operation: 'Find',
          type: 'Image',
          constraints: { _uniqueid: ['==', imageId] }
        },
        connectionProperties: {
          description: 'Test connection between image and descriptor',
          confidence: 0.95
        }
      });

      // Verify we can find the connection
      const connections = await client.connections.findConnections({
        with_class: 'DescribesImage',
        constraints: {
          description: ['==', 'Test connection between image and descriptor']
        }
      });

      expect(connections.length).toBe(1);
      connectionId = connections[0]._uniqueid!;
      expect(connections[0].class).toBe('DescribesImage');
      expect(connections[0].confidence).toBe(0.95);
    });

    afterAll(async () => {
      // Clean up in case test failed
      if (connectionId) {
        await client.connections.deleteConnection({
          with_class: 'DescribesImage',
          constraints: { _uniqueid: ['==', connectionId] }
        });
      }

      if (imageId) {
        await client.images.deleteImage({ _uniqueid: ['==', imageId] });
      }

      if (descriptorId) {
        await client.descriptors.deleteDescriptor({
          set: testDescriptorSet.name,
          constraints: { _uniqueid: ['==', descriptorId] }
        });
      }

      // Clean up the descriptor set
      try {
        await client.descriptorSets.deleteDescriptorSet({
          with_name: testDescriptorSet.name
        });
      } catch (error) {
        // Ignore cleanup errors
      }
    });
  });

  describe('Frame-to-Image Connection', () => {
    const testVideoPath = 'testdata/test.mp4';
    const testImagePath = 'testdata/test.png';
    
    const testVideoProperties = {
      name: 'test-video-for-frame-image',
      description: 'A test video for frame-to-image connection',
      fps: 30,
      duration: 10000000 // 10 seconds in microseconds
    };

    const testImageProperties = {
      name: 'test-image-for-frame',
      description: 'A test image for frame connection'
    };

    const testFrameProperties = {
      description: 'A test frame for image connection',
      scene: 'test scene'
    };

    let videoId: string;
    let frameId: string;
    let imageId: string;
    let connectionId: string;

    beforeAll(async () => {
      // Clean up any existing test videos
      await client.videos.findVideos({ 
        constraints: { name: ['==', testVideoProperties.name] }
      }).then(videos => {
        return Promise.all(videos.map(video => 
          client.videos.deleteVideo({ _uniqueid: ["==", video._uniqueid!] })
        ));
      });

      // Clean up any existing test images
      await client.images.findImages({ 
        constraints: { name: ['==', testImageProperties.name] }
      }).then(images => {
        return Promise.all(images.map(image => 
          client.images.deleteImage({ _uniqueid: ["==", image._uniqueid!] })
        ));
      });

      // Clean up any existing test connections
      await client.connections.findAndDeleteConnections({
        with_class: 'MatchesImage',
        constraints: {
          description: ['==', 'Test connection between frame and image']
        }
      });
    });

    test('should create frame, image, and connect them', async () => {
      // Create the video first
      const videoBuffer = fs.readFileSync(testVideoPath);
      await client.videos.addVideo({
        blob: videoBuffer,
        properties: testVideoProperties
      });

      // Find the created video
      const [video] = await client.videos.findVideos({ 
        constraints: { name: ['==', testVideoProperties.name] }
      });
      expect(video).toBeDefined();
      expect(video._uniqueid).toBeDefined();
      videoId = video._uniqueid!;

      // Create a frame from the video
      const frame = await client.frames.addFrame({
        video_ref: videoId,
        frame_number: 30, // 1 second at 30fps
        properties: testFrameProperties,
        label: 'test-frame-for-image'
      });

      // Find the created frame
      const [foundFrame] = await client.frames.findFrames({
        constraints: {
          _label: ["==", "test-frame-for-image"]
        },
        uniqueids: true
      });
      console.log("!!!!!!", foundFrame);
      expect(foundFrame).toBeDefined();
      expect(foundFrame._uniqueid).toBeDefined();
      frameId = foundFrame._uniqueid!;

      // Create the image
      const imageBuffer = fs.readFileSync(testImagePath);
      await client.images.addImage({
        blob: imageBuffer,
        properties: testImageProperties
      });

      // Find the created image
      const [image] = await client.images.findImages({ 
        constraints: { name: ['==', testImageProperties.name] }
      });
      expect(image).toBeDefined();
      expect(image._uniqueid).toBeDefined();
      imageId = image._uniqueid!;

      // Create the connection using addConnectionWithObjects
      await client.connections.addConnectionWithObjects({
        connectionClass: 'MatchesImage',
        sourceObject: {
          operation: 'Find',
          type: 'Frame',
          constraints: { _uniqueid: ['==', frameId] }
        },
        targetObject: {
          operation: 'Find',
          type: 'Image',
          constraints: { _uniqueid: ['==', imageId] }
        },
        connectionProperties: {
          description: 'Test connection between frame and image',
          confidence: 0.95
        }
      });

      // Verify we can find the connection
      const connections = await client.connections.findConnections({
        with_class: 'MatchesImage',
        constraints: {
          description: ['==', 'Test connection between frame and image']
        }
      });

      expect(connections.length).toBe(1);
      connectionId = connections[0]._uniqueid!;
      expect(connections[0].class).toBe('MatchesImage');
      expect(connections[0].confidence).toBe(0.95);
    });

    afterAll(async () => {
      // Clean up in case test failed
      if (connectionId) {
        await client.connections.deleteConnection({
          with_class: 'MatchesImage',
          constraints: { _uniqueid: ['==', connectionId] }
        });
      }

      if (videoId) {
        await client.videos.deleteVideo({ 
          _uniqueid: ['==', videoId] 
        });
      }

      if (imageId) {
        await client.images.deleteImage({ 
          _uniqueid: ['==', imageId] 
        });
      }

      if (frameId) {
        await client.frames.deleteFrame({
          _uniqueid: ['==', frameId]
        });
      }
    });
  });
}); 