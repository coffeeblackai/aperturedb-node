import { BaseClient } from './base.js';
import { PolygonMetadata, CreatePolygonInput, FindPolygonOptions, QueryOptions } from './types.js';
import { Logger } from './utils/logger.js';

export class PolygonClient {
  private baseClient: BaseClient;

  constructor(baseClient: BaseClient) {
    this.baseClient = baseClient;
  }

  private isChainedAddPolygonResponse(response: unknown): response is [
    { FindImage: { returned: number; status: number; entities: Array<{ _uniqueid: string }> } },
    { AddPolygon: { status: number; _uniqueid?: string } }
  ] {
    if (!Array.isArray(response)) {
      Logger.error('Response is not an array:', response);
      throw new Error('Invalid response format');
    }
    
    if (response.length !== 2) {
      Logger.error('Response array length is not 2:', response.length);
      throw new Error('Invalid response length');
    }
    
    if (!('FindImage' in response[0])) {
      Logger.error('First element missing FindImage:', response[0]);
      throw new Error('Invalid response format');
    }
    
    if (!('AddPolygon' in response[1])) {
      Logger.error('Second element missing AddPolygon:', response[1]);
      throw new Error('Invalid response format');
    }

    const findImage = response[0].FindImage;
    const addPolygon = response[1].AddPolygon;
    
    if (typeof findImage !== 'object' || findImage === null) {
      Logger.error('FindImage is not an object:', findImage);
      throw new Error('Invalid FindImage format');
    }
    
    if (typeof addPolygon !== 'object' || addPolygon === null) {
      Logger.error('AddPolygon is not an object:', addPolygon);
      throw new Error('Invalid AddPolygon format');
    }
    
    // if (!Array.isArray(findImage.entities) || findImage.entities.length === 0) {
    //   Logger.error('FindImage entities is not an array or is empty:', findImage.entities);
    //   throw new Error('No matching image found');
    // }
    

    return true;
  }

  async addPolygon(input: Omit<CreatePolygonInput, 'image_ref'> & { constraints: Record<string, any> }): Promise<PolygonMetadata> {
    await this.baseClient.ensureAuthenticated();
    
    if (!input.constraints || Object.keys(input.constraints).length === 0) {
      throw new Error('constraints are required for addPolygon');
    }

    if (!input.points || !input.points.length) {
      throw new Error('points are required for addPolygon');
    }

    const query = [{
      "FindImage": {
        "_ref": 1,
        "unique": true,
        "blobs": false,
        "constraints": input.constraints
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