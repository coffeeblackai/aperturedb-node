import { AutoProcessor, CLIPVisionModelWithProjection, RawImage } from '@xenova/transformers';
import { apertureClient } from './client';
import { FindSimilarImagesResponse, QueryResponse } from '../../types/aperture';

interface CLIPEmbeddingResult {
  image_embeds: {
    data: Float32Array;
  };
}

class ImageSearchService {
  private visionModel: CLIPVisionModelWithProjection | null = null;
  private processor: AutoProcessor | null = null;
  private readonly descriptorSet: string = 'elements';

  constructor() {
    console.log('ImageSearchService initialized');
  }

  private async initCLIP(): Promise<void> {
    if (!this.visionModel || !this.processor) {
      console.log('Initializing CLIP vision model and processor...');
      
      // Load processor and vision model
      const [loadedProcessor, loadedModel] = await Promise.all([
        AutoProcessor.from_pretrained('Xenova/clip-vit-base-patch32'),
        CLIPVisionModelWithProjection.from_pretrained('Xenova/clip-vit-base-patch32')
      ]);

      this.processor = loadedProcessor;
      this.visionModel = loadedModel;
      
      console.log('CLIP vision model and processor initialized successfully');
    }
  }

  async generateClipEmbedding(imageBuffer: Buffer): Promise<Float32Array> {
    console.log('Generating CLIP embedding...');
    await this.initCLIP();
    
    try {
      if (!this.processor || !this.visionModel) {
        throw new Error('CLIP model not initialized');
      }

      // Create a blob from the buffer and convert to RawImage
      const blob = new Blob([imageBuffer], { type: 'image/jpeg' });
      const image = await RawImage.fromBlob(blob);
      
      // Process the image
      const imageInputs = await this.processor(image);
      
      // Generate embeddings
      const output = await this.visionModel(imageInputs) as CLIPEmbeddingResult;
      
      // Convert the tensor to Float32Array
      const embeddings = Array.from(output.image_embeds.data);
      
      console.log('CLIP embedding generated successfully');
      return new Float32Array(embeddings);
    } catch (error) {
      console.error('Error generating CLIP embedding:', error);
      throw error;
    }
  }

  async findSimilarImages(
    descriptorSet: string,
    queryImageBuffer: Buffer,
    k: number = 5
  ): Promise<FindSimilarImagesResponse> {
    try {
      console.log(`Finding similar images in descriptor set '${descriptorSet}' with k=${k}`);
      
      // Ensure we're authenticated
      if (!apertureClient.sessionToken) {
        await apertureClient.authenticate();
      }
      
      // Generate embedding for query image
      const queryEmbedding = await this.generateClipEmbedding(queryImageBuffer);
      console.log('Query embedding generated');
      
      // Convert embedding to bytes
      const queryEmbeddingBytes = new Uint8Array(queryEmbedding.buffer);

      // Updated query to follow the connection chain: Descriptor -> Image -> MacIcon
      const query = [{
        "FindDescriptor": {
          "set": descriptorSet,
          "k_neighbors": k,
          "distances": true,
          "labels": true,
          "_ref": 1,
          "results": {
            "all_properties": true
          }
        }
      }, {
        "FindImage": {
          "is_connected_to": {
            "ref": 1,
            "direction": "any",
          },
          "_ref": 2,
          "results": {
            "all_properties": true
          }
        }
      }];

      // Use the client's request method
      const result = await apertureClient.request(
        query, 
        [new Blob([queryEmbeddingBytes])]
      );

      console.log("Similar images found:", JSON.stringify(result, null, 2));
      
      return {
        response: result.json,
        blobs: result.blobs || []
      };

    } catch (error) {
      console.error("Error finding similar images:", error);
      console.error("Error details:", {
        message: error instanceof Error ? error.message : String(error),
        stack: error instanceof Error ? error.stack : undefined,
        descriptorSet,
        k
      });
      throw error;
    }
  }
}

// Export singleton instance
export const imageSearch = new ImageSearchService(); 