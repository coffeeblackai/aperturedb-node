import { BaseClient } from './base';
import {
  ImageMetadata,
  CreateImageInput,
  FindImageOptions,
  QueryOptions
} from './types';

export class ImageClient {
  private baseClient: BaseClient;

  constructor(baseClient: BaseClient) {
    this.baseClient = baseClient;
  }

  private isAddImageResponse(response: unknown): response is { AddImage: { status: number } }[] {
    return Array.isArray(response) && 
           response.length > 0 && 
           'AddImage' in response[0] &&
           typeof response[0].AddImage === 'object' &&
           response[0].AddImage !== null &&
           'status' in response[0].AddImage;
  }

  private isFindImageResponse(response: unknown): response is { FindImage: { entities: ImageMetadata[]; returned: number; status: number } }[] {
    return Array.isArray(response) && 
           response.length > 0 && 
           'FindImage' in response[0] &&
           typeof response[0].FindImage === 'object' &&
           response[0].FindImage !== null &&
           'entities' in response[0].FindImage &&
           Array.isArray(response[0].FindImage.entities);
  }

  private isDeleteImageResponse(response: unknown): response is { DeleteImage: { status: number } }[] {
    return Array.isArray(response) && 
           response.length > 0 && 
           'DeleteImage' in response[0] &&
           typeof response[0].DeleteImage === 'object' &&
           response[0].DeleteImage !== null &&
           'status' in response[0].DeleteImage;
  }

  async addImage(input: CreateImageInput): Promise<ImageMetadata> {
    await this.baseClient.ensureAuthenticated();
    
    if (!input.url && !input.blob) {
      throw new Error('Either url or blob is required for addImage');
    }

    const query = [{
      "AddImage": {
        "url": input.url,
        "properties": input.properties || {}
      }
    }];

    const blobs = input.blob ? [input.blob] : [];
    const [response] = await this.baseClient.query(query, blobs);
    if (!this.isAddImageResponse(response)) {
      throw new Error('Invalid response from server');
    }

    return {
      ...input.properties || {},
      url: input.url,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    };
  }

  async findImage(options?: FindImageOptions): Promise<ImageMetadata> {
    await this.baseClient.ensureAuthenticated();
    
    const query = [{
      "FindImage": {
        "constraints": options?.constraints,
        "results": {
          "all_properties": true,
          ...options?.results
        },
        "uniqueids": options?.uniqueids
      }
    }];

    const [response] = await this.baseClient.query(query, []);
    if (!this.isFindImageResponse(response) || !response[0].FindImage.entities.length) {
      throw new Error('No images found');
    }

    return response[0].FindImage.entities[0];
  }

  async findImages(options?: FindImageOptions & QueryOptions): Promise<ImageMetadata[]> {
    await this.baseClient.ensureAuthenticated();
    
    const query = [{
      "FindImage": {
        "constraints": options?.constraints,
        "results": {
          "all_properties": true,
          ...options?.results
        },
        "uniqueids": options?.uniqueids,
        "limit": options?.limit,
        "offset": options?.offset,
        "sort": options?.sort
      }
    }];

    const [response] = await this.baseClient.query(query, []);
    if (!this.isFindImageResponse(response)) {
      return [];
    }

    return response[0].FindImage.entities || [];
  }

  async deleteImage(constraints: Record<string, any>): Promise<void> {
    await this.baseClient.ensureAuthenticated();
    
    const query = [{
      "DeleteImage": {
        "constraints": constraints
      }
    }];

    const [response] = await this.baseClient.query(query, []);
    if (!this.isDeleteImageResponse(response)) {
      throw new Error('Invalid response from server');
    }
  }
} 