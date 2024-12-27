import { BaseClient } from './base';
import {
  PolygonMetadata,
  CreatePolygonInput,
  FindPolygonOptions,
  QueryOptions,
  DeletePolygonOptions
} from './types';

export class PolygonClient {
  private baseClient: BaseClient;

  constructor(baseClient: BaseClient) {
    this.baseClient = baseClient;
  }

  private isChainedAddPolygonResponse(response: unknown): response is [
    { FindImage: { returned: number; status: number } },
    { AddPolygon: { status: number; _uniqueid?: string } }
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
    
    if (!response[1] || !('AddPolygon' in response[1])) {
      console.error('Second element missing AddPolygon:', response[1]);
      return false;
    }

    const findImage = response[0].FindImage;
    const addPolygon = response[1].AddPolygon;
    
    if (!findImage || typeof findImage !== 'object') {
      console.error('FindImage is not an object:', findImage);
      return false;
    }
    
    if (!addPolygon || typeof addPolygon !== 'object') {
      console.error('AddPolygon is not an object:', addPolygon);
      return false;
    }
    
    if (!('status' in findImage) || !('returned' in findImage)) {
      console.error('FindImage missing required properties:', findImage);
      return false;
    }
    
    if (!('status' in addPolygon)) {
      console.error('AddPolygon missing required properties:', addPolygon);
      return false;
    }

    return true;
  }

  async addPolygon(input: Omit<CreatePolygonInput, 'image_ref'> & { imageId: string }): Promise<PolygonMetadata> {
    await this.baseClient.ensureAuthenticated();
    
    if (!input.imageId) {
      throw new Error('imageId is required for addPolygon');
    }

    if (!input.points || !input.points.length) {
      throw new Error('points are required for addPolygon');
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
      "AddPolygon": {
        "image_ref": 1,
        "polygons": [input.points],
        "properties": input.properties || {}
      }
    }];

    const [response] = await this.baseClient.query(query, []);
    if (!this.isChainedAddPolygonResponse(response)) {
      throw new Error('Invalid response from server');
    }

    if (response[0].FindImage.status !== 0 || response[1].AddPolygon.status !== 0) {
      throw new Error('Failed to create polygon');
    }

    return {
      image_ref: input.imageId,
      points: input.points,
      properties: input.properties || {},
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
      _uniqueid: response[1].AddPolygon._uniqueid
    };
  }

  async findPolygon(options?: FindPolygonOptions): Promise<PolygonMetadata> {
    await this.baseClient.ensureAuthenticated();
    
    const query = [{
      "FindPolygon": {
        "constraints": options?.constraints,
        "results": {
          "all_properties": true,
          ...options?.results
        },
        "uniqueids": options?.uniqueids
      }
    }];

    type FindPolygonResponse = [{
      FindPolygon: {
        entities: PolygonMetadata[];
        returned: number;
        status: number;
      }
    }];

    const [response] = await this.baseClient.query<FindPolygonResponse>(query, []);
    return response[0].FindPolygon.entities[0];
  }

  async findPolygons(options?: FindPolygonOptions & QueryOptions): Promise<PolygonMetadata[]> {
    await this.baseClient.ensureAuthenticated();
    
    const query = [{
      "FindPolygon": {
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

    type FindPolygonResponse = [{
      FindPolygon: {
        entities: PolygonMetadata[];
        returned: number;
        status: number;
      }
    }];

    const [response] = await this.baseClient.query<FindPolygonResponse>(query, []);
    return response[0].FindPolygon.entities || [];
  }

  async deletePolygon(constraints: Record<string, any>): Promise<void> {
    await this.baseClient.ensureAuthenticated();
    
    const query = [{
      "DeletePolygon": {
        "constraints": constraints,
      }
    }];

    type DeletePolygonResponse = [{
      DeletePolygon: {
        status: number;
      }
    }];

    await this.baseClient.query<DeletePolygonResponse>(query, []);
  }
} 