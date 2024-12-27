import { BaseClient } from './base';
import {
  DescriptorSet,
  QueryOptions,
  DescriptorSetResponse
} from './types';

export class DescriptorSetClient {
  private baseClient: BaseClient;

  constructor(baseClient: BaseClient) {
    this.baseClient = baseClient;
  }

  private isAddDescriptorSetResponse(response: unknown): response is { AddDescriptorSet: { status: number } }[] {
    return Array.isArray(response) && 
           response.length > 0 && 
           'AddDescriptorSet' in response[0] &&
           typeof response[0].AddDescriptorSet === 'object' &&
           response[0].AddDescriptorSet !== null &&
           'status' in response[0].AddDescriptorSet;
  }

  private isDeleteDescriptorSetResponse(response: unknown): response is { DeleteDescriptorSet: { status: number } }[] {
    return Array.isArray(response) && 
           response.length > 0 && 
           'DeleteDescriptorSet' in response[0] &&
           typeof response[0].DeleteDescriptorSet === 'object' &&
           response[0].DeleteDescriptorSet !== null &&
           'status' in response[0].DeleteDescriptorSet;
  }

  private isFindDescriptorSetResponse(response: unknown): response is { FindDescriptorSet: { entities: DescriptorSetResponse[]; returned: number; status: number } }[] {
    if (!Array.isArray(response) || response.length === 0) return false;
    
    const first = response[0];
    if (!first || typeof first !== 'object' || !('FindDescriptorSet' in first)) return false;
    
    const findResponse = first.FindDescriptorSet;
    if (!findResponse || typeof findResponse !== 'object') return false;

    // Check for required fields
    if (!('entities' in findResponse) || !Array.isArray(findResponse.entities)) return false;
    if (!('status' in findResponse) || typeof findResponse.status !== 'number') return false;
    if (!('returned' in findResponse) || typeof findResponse.returned !== 'number') return false;

    // Check each entity has the required fields based on the query options
    return findResponse.entities.every((entity: Record<string, unknown>) => 
      typeof entity === 'object' && 
      entity !== null && 
      '_name' in entity &&
      (
        // Basic fields
        typeof entity._name === 'string' &&
        (!('_dimensions' in entity) || typeof entity._dimensions === 'number') &&
        (!('_count' in entity) || typeof entity._count === 'number') &&
        (!('_uniqueid' in entity) || typeof entity._uniqueid === 'string') &&
        // Array fields
        (!('_engines' in entity) || Array.isArray(entity._engines)) &&
        (!('_metrics' in entity) || Array.isArray(entity._metrics))
      )
    );
  }

  async addDescriptorSet(input: Partial<DescriptorSet>): Promise<DescriptorSetResponse> {
    await this.baseClient.ensureAuthenticated();
    
    if (!input.name) {
      throw new Error('name is required for addDescriptorSet');
    }

    const query = [{
      "AddDescriptorSet": {
        "name": input.name,
        "dimensions": input.dimensions || 512,
        "metric": input.metric || 'L2',
        "engine": input.engine || 'HNSW'
      }
    }];

    const [response] = await this.baseClient.query(query, []);
    if (!this.isAddDescriptorSetResponse(response)) {
      throw new Error('Invalid response from server');
    }

    return {
      name: input.name,
      dimensions: input.dimensions || 512,
      metric: input.metric || 'L2',
      engine: input.engine || 'HNSW',
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    };
  }

  async findDescriptorSet(options: {
    with_name?: string;
    constraints?: Record<string, any>;
    results?: Record<string, any>;
    uniqueids?: boolean;
    engines?: boolean;
    metrics?: boolean;
    dimensions?: boolean;
    counts?: boolean;
    is_connected_to?: any[];
    group_by_source?: boolean;
  } & QueryOptions = {}): Promise<DescriptorSetResponse[]> {
    await this.baseClient.ensureAuthenticated();

    const query = [{
      "FindDescriptorSet": {
        "with_name": options.with_name,
        "constraints": options.constraints,
        "results": {
          "all_properties": true,
          ...options.results
        },
        "uniqueids": options.uniqueids,
        "engines": options.engines,
        "metrics": options.metrics,
        "dimensions": options.dimensions,
        "counts": options.counts,
        "is_connected_to": options.is_connected_to,
        "group_by_source": options.group_by_source,
        "limit": options.limit,
        "offset": options.offset,
        "sort": options.sort
      }
    }];

    const [response] = await this.baseClient.query(query, []);
    if (!this.isFindDescriptorSetResponse(response)) {
      return [];
    }

    return response[0].FindDescriptorSet.entities;
  }

  async findDescriptorSets(options: QueryOptions = {}): Promise<DescriptorSetResponse[]> {
    await this.baseClient.ensureAuthenticated();

    const query = [{
      "FindDescriptorSet": {
        "limit": options.limit,
        "offset": options.offset,
        "sort": options.sort
      }
    }];

    const [response] = await this.baseClient.query(query, []);
    if (!this.isFindDescriptorSetResponse(response)) {
      console.error('Invalid response from server when finding descriptor sets');
    }

    return response[0].FindDescriptorSet.entities;
  }

  async deleteDescriptorSet(options: { with_name?: string; constraints?: Record<string, any> } = {}): Promise<void> {
    await this.baseClient.ensureAuthenticated();
    
    const query = [{
      "DeleteDescriptorSet": {
        with_name: options.with_name,
        constraints: options.constraints
      }
    }];

    const [response] = await this.baseClient.query(query, []);
    if (!this.isDeleteDescriptorSetResponse(response)) {
      throw new Error('Invalid response from server');
    }
  }
} 