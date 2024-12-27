import { BaseClient } from './base.js';
import {
  BoundingBoxMetadata,
  CreateBoundingBoxInput,
  FindBoundingBoxOptions,
  QueryOptions
} from './types.js';

export class BoundingBoxClient {
  private baseClient: BaseClient;

  constructor(baseClient: BaseClient) {
    this.baseClient = baseClient;
  }

  private isChainedAddBoundingBoxResponse(response: unknown): response is [
    { FindImage: { returned: number; status: number } },
    { AddBoundingBox: { status: number; _uniqueid?: string } }
  ] {
    if (!Array.isArray(response)) {
      console.error('Response is not an array:', response);
      return false;
    }
    
    if (response.length !== 2) {
      console.error('Response array length is not 2:', response.length);
      return false;
    }
    
    if (!response[0] || !('FindImage' in response[0])) {
      console.error('First element missing FindImage:', response[0]);
      return false;
    }
    
    if (!response[1] || !('AddBoundingBox' in response[1])) {
      console.error('Second element missing AddBoundingBox:', response[1]);
      return false;
    }

    const findImage = response[0].FindImage;
    const addBoundingBox = response[1].AddBoundingBox;
    
    if (!findImage || typeof findImage !== 'object') {
      console.error('FindImage is not an object:', findImage);
      return false;
    }
    
    if (!addBoundingBox || typeof addBoundingBox !== 'object') {
      console.error('AddBoundingBox is not an object:', addBoundingBox);
      return false;
    }
    
    if (!('status' in findImage) || !('returned' in findImage)) {
      console.error('FindImage missing required properties:', findImage);
      return false;
    }
    
    if (!('status' in addBoundingBox)) {
      console.error('AddBoundingBox missing required properties:', addBoundingBox);
      return false;
    }

    return true;
  }

  async addBoundingBox(input: Omit<CreateBoundingBoxInput, 'image_ref'> & { imageId: string }): Promise<BoundingBoxMetadata> {
    await this.baseClient.ensureAuthenticated();
    
    if (typeof input.x !== 'number' || typeof input.y !== 'number' || 
        typeof input.width !== 'number' || typeof input.height !== 'number' ||
        input.x === 0 || input.y === 0 || input.width === 0 || input.height === 0) {
      throw new Error('x, y, width, and height are required for addBoundingBox');
    }

    if (!input.imageId) {
      throw new Error('imageId is required for addBoundingBox');
    }

    const query = [{
      "FindImage": {
        "_ref": 1,
        "unique": true,
        "blobs": false,
        "constraints": {
          "_uniqueid": ["==", input.imageId]
        }
      }
    }, {
      "AddBoundingBox": {
        "image_ref": 1,
        "rectangle": {
          "x": input.x,
          "y": input.y,
          "width": input.width,
          "height": input.height
        },
        "properties": input.properties || {}
      }
    }];

    const [response] = await this.baseClient.query(query, []);
    if (!this.isChainedAddBoundingBoxResponse(response)) {
      throw new Error('Invalid response from server');
    }

    if (response[0].FindImage.status !== 0 || response[1].AddBoundingBox.status !== 0) {
      throw new Error('Failed to create bounding box');
    }

    return {
      image_ref: input.imageId,
      x: input.x,
      y: input.y,
      width: input.width,
      height: input.height,
      properties: input.properties || {},
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
      _uniqueid: response[1].AddBoundingBox._uniqueid
    };
  }

  async findBoundingBox(options?: FindBoundingBoxOptions): Promise<BoundingBoxMetadata> {
    await this.baseClient.ensureAuthenticated();
    
    const query = [{
      "FindBoundingBox": {
        "constraints": options?.constraints,
        "results": {
          "all_properties": true,
          ...options?.results
        },
        "uniqueids": options?.uniqueids
      }
    }];

    type FindBoundingBoxResponse = [{
      FindBoundingBox: {
        entities: BoundingBoxMetadata[];
        returned: number;
        status: number;
      }
    }];

    const [response] = await this.baseClient.query<FindBoundingBoxResponse>(query, []);
    return response[0].FindBoundingBox.entities[0];
  }

  async findBoundingBoxes(options?: FindBoundingBoxOptions & QueryOptions): Promise<BoundingBoxMetadata[]> {
    await this.baseClient.ensureAuthenticated();
    
    const query = [{
      "FindBoundingBox": {
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

    type FindBoundingBoxResponse = [{
      FindBoundingBox: {
        entities: BoundingBoxMetadata[];
        returned: number;
        status: number;
      }
    }];

    const [response] = await this.baseClient.query<FindBoundingBoxResponse>(query, []);
    return response[0].FindBoundingBox.entities || [];
  }

  async deleteBoundingBox(constraints: Record<string, any>): Promise<void> {
    await this.baseClient.ensureAuthenticated();
    
    const query = [{
      "DeleteBoundingBox": {
        "constraints": constraints
      }
    }];

    type DeleteBoundingBoxResponse = [{
      DeleteBoundingBox: {
        status: number;
      }
    }];

    const [response] = await this.baseClient.query<DeleteBoundingBoxResponse>(query, []);
    
    if (!response || !('DeleteBoundingBox' in response[0])) {
      throw new Error('Invalid response from server for delete operation');
    }
    
    if (response[0].DeleteBoundingBox.status !== 0) {
      throw new Error('Failed to delete bounding box');
    }
  }
} 