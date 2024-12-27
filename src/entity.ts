import { BaseClient } from './base.js';
import { EntityMetadata, CreateEntityInput, FindEntityOptions, QueryOptions } from './types.js';

export class EntityClient {
  private baseClient: BaseClient;

  constructor(baseClient: BaseClient) {
    this.baseClient = baseClient;
  }

  private isFindEntityResponse(response: unknown): response is { FindEntity: { entities: Omit<EntityMetadata, 'class'>[]; returned: number; status: number } }[] {
    return Array.isArray(response) && 
           response.length > 0 && 
           'FindEntity' in response[0] &&
           typeof response[0].FindEntity === 'object' &&
           response[0].FindEntity !== null &&
           'entities' in response[0].FindEntity &&
           Array.isArray(response[0].FindEntity.entities);
  }

  private isAddEntityResponse(response: unknown): response is { AddEntity: { status: number } }[] {
    return Array.isArray(response) && 
           response.length > 0 && 
           'AddEntity' in response[0] &&
           typeof response[0].AddEntity === 'object' &&
           response[0].AddEntity !== null &&
           'status' in response[0].AddEntity &&
           typeof response[0].AddEntity.status === 'number';
  }

  private isDeleteEntityResponse(response: unknown): response is { DeleteEntity: { status: number } }[] {
    return Array.isArray(response) && 
           response.length > 0 && 
           'DeleteEntity' in response[0] &&
           typeof response[0].DeleteEntity === 'object' &&
           response[0].DeleteEntity !== null &&
           'status' in response[0].DeleteEntity;
  }

  private isUpdateEntityResponse(response: unknown): response is { UpdateEntity: { status: number } }[] {
    return Array.isArray(response) && 
           response.length > 0 && 
           'UpdateEntity' in response[0] &&
           typeof response[0].UpdateEntity === 'object' &&
           response[0].UpdateEntity !== null &&
           'status' in response[0].UpdateEntity;
  }

  async findEntities(options: { with_class?: string; constraints?: Record<string, any>; results?: Record<string, any>; uniqueids?: string[] } & QueryOptions = {}): Promise<EntityMetadata[]> {
    await this.baseClient.ensureAuthenticated();

    const query = [{
      "FindEntity": {
        "with_class": options.with_class,
        "constraints": options.constraints,
        "results": {
          "all_properties": true,
          ...options.results
        },
        "uniqueids": options.uniqueids,
        "limit": options.limit,
        "offset": options.offset,
        "sort": options.sort
      }
    }];

    const [response] = await this.baseClient.query(query, []);
    if (!this.isFindEntityResponse(response)) {
      return [];
    }

    return response[0].FindEntity.entities.map(entity => ({
      ...entity,
      class: options.with_class || 'Entity'
    }));
  }

  async addEntity(entityClass: string, properties: Record<string, any> = {}): Promise<EntityMetadata> {
    await this.baseClient.ensureAuthenticated();

    const query = [{
      "AddEntity": {
        "class": entityClass,
        "properties": properties
      }
    }];

    const [response] = await this.baseClient.query(query, []);
    if (!this.isAddEntityResponse(response)) {
      throw new Error('Invalid response from server');
    }

    return {
      class: entityClass,
      properties: properties
    };
  }

  async deleteEntity(options: { class?: string; constraints?: Record<string, any> } = {}): Promise<void> {
    await this.baseClient.ensureAuthenticated();

    const query = [{
      "DeleteEntity": {
        "with_class": options.class,
        "constraints": options.constraints
      }
    }];

    const [response] = await this.baseClient.query(query, []);
    if (!this.isDeleteEntityResponse(response)) {
      throw new Error('Invalid response from server');
    }
  }

  async updateEntity(options: { with_class?: string; properties?: Record<string, any>; remove_props?: string[]; constraints?: Record<string, any> } = {}): Promise<void> {
    await this.baseClient.ensureAuthenticated();

    const query = [{
      "UpdateEntity": {
        "with_class": options.with_class,
        "properties": options.properties,
        "remove_props": options.remove_props,
        "constraints": options.constraints
      }
    }];

    const [response] = await this.baseClient.query(query, []);
    if (!this.isUpdateEntityResponse(response)) {
      throw new Error('Invalid response from server');
    }
  }
}