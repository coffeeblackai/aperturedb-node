import { BaseClient } from './base';
import {
  Connection,
  QueryOptions,
  Reference,
  Entity
} from './types';

// Define possible operation responses
type OperationResponse = {
  status: number;
  _uniqueid?: string;
  [key: string]: any;
};

type ChainedOperationResponse = {
  AddEntity?: OperationResponse;
  AddConnection?: OperationResponse;
  AddVideo?: OperationResponse;
  AddImage?: OperationResponse;
  AddFrame?: OperationResponse;
  AddClip?: OperationResponse;
  FindEntity?: OperationResponse;
  FindConnection?: OperationResponse;
  FindVideo?: OperationResponse;
  FindImage?: OperationResponse;
  FindFrame?: OperationResponse;
  FindClip?: OperationResponse;
  [key: string]: OperationResponse | undefined;
};

export class ConnectionClient {
  private baseClient: BaseClient;

  constructor(baseClient: BaseClient) {
    this.baseClient = baseClient;
  }

  private isOperationResponse(obj: unknown): obj is OperationResponse {
    return typeof obj === 'object' &&
           obj !== null &&
           'status' in obj &&
           typeof obj.status === 'number';
  }

  private isChainedResponse(response: unknown): response is ChainedOperationResponse[] {
    return Array.isArray(response) &&
           response.every(op => {
             const key = Object.keys(op)[0];
             return key && this.isOperationResponse(op[key as keyof ChainedOperationResponse]);
           });
  }

  async findConnections(options: { 
    with_class?: string; 
    constraints?: Record<string, any>; 
    results?: Record<string, any>; 
    uniqueids?: string[];
    src?: string | Reference;
    dst?: string | Reference;
  } & QueryOptions = {}): Promise<Connection[]> {
    await this.baseClient.ensureAuthenticated();

    const query = [{
      "FindConnection": {
        "with_class": options.with_class,
        "constraints": options.constraints,
        "results": {
          "all_properties": true,
          ...options.results
        },
        "uniqueids": options.uniqueids,
        "src": options.src,
        "dst": options.dst,
        "limit": options.limit,
        "offset": options.offset,
        "sort": options.sort
      }
    }];

    const [response] = await this.baseClient.query(query, []);
    if (!this.isChainedResponse(response) || 
        !response[0].FindConnection ||
        !('connections' in response[0].FindConnection)) {
      return [];
    }

    return response[0].FindConnection.connections.map((connection: Record<string, any>) => ({
      ...connection,
      class: options.with_class || connection.class
    }));
  }

  async addConnection(input: { 
    class: string;
    src: string | Reference;
    dst: string | Reference;
    properties?: Record<string, any>;
    if_not_found?: boolean;
  }): Promise<Connection> {
    await this.baseClient.ensureAuthenticated();

    const query = [{
      "AddConnection": {
        "class": input.class,
        "src": input.src,
        "dst": input.dst,
        "properties": input.properties || {},
        "if_not_found": input.if_not_found
      }
    }];

    const [response] = await this.baseClient.query(query, []);
    if (!this.isChainedResponse(response) || 
        !response[0].AddConnection ||
        response[0].AddConnection.status !== 0) {
      throw new Error('Invalid response from server');
    }

    return {
      class: input.class,
      src: input.src,
      dst: input.dst,
      properties: input.properties || {},
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    };
  }

  async updateConnection(input: {
    with_class?: string;
    constraints?: Record<string, any>;
    properties?: Record<string, any>;
    remove_props?: string[];
  }): Promise<void> {
    await this.baseClient.ensureAuthenticated();

    const query = [{
      "UpdateConnection": {
        ...(input.with_class && { "with_class": input.with_class }),
        ...(input.constraints && { "constraints": input.constraints }),
        ...(input.properties && { "properties": input.properties }),
        ...(input.remove_props && { "remove_props": input.remove_props })
      }
    }];

    const [response] = await this.baseClient.query(query, []);
    if (!this.isChainedResponse(response) ||
        !response[0].UpdateConnection ||
        response[0].UpdateConnection.status !== 0) {
      throw new Error('Failed to update connection');
    }
  }

  async deleteConnection(input: { 
    ref?: number;
    with_class?: string;
    constraints?: Record<string, any>;
  }): Promise<void> {
    await this.baseClient.ensureAuthenticated();

    const query = [{
      "DeleteConnection": {
        ...(input.ref && { "ref": input.ref }),
        ...(input.with_class && { "with_class": input.with_class }),
        ...(input.constraints && { "constraints": input.constraints })
      }
    }];

    const [response] = await this.baseClient.query(query, []);
    if (!this.isChainedResponse(response) ||
        !response[0].DeleteConnection ||
        response[0].DeleteConnection.status !== 0) {
      throw new Error('Failed to delete connection');
    }
  }

  // Helper method for complex delete operations that require finding first
  async findAndDeleteConnections(input: {
    with_class?: string;
    src?: string | Reference;
    dst?: string | Reference;
    constraints?: Record<string, any>;
  }): Promise<void> {
    await this.baseClient.ensureAuthenticated();

    const query = [{
      "FindConnection": {
        "_ref": 1,
        ...(input.with_class && { "with_class": input.with_class }),
        ...(input.src && { "src": input.src }),
        ...(input.dst && { "dst": input.dst }),
        ...(input.constraints && { "constraints": input.constraints })
      }
    }, {
      "DeleteConnection": {
        "ref": 1
      }
    }];

    const [response] = await this.baseClient.query(query, []);
    if (!this.isChainedResponse(response) ||
        !response[0].FindConnection ||
        !response[1].DeleteConnection ||
        response[0].FindConnection.status !== 0 ||
        response[1].DeleteConnection.status !== 0) {
      // Only throw if there's an actual error, not if no connections were found
      if (response[0].FindConnection?.status !== 0 ||
          response[1].DeleteConnection?.status !== 0) {
        throw new Error('Failed to find and delete connections');
      }
    }
  }

  async addConnectionWithObjects(input: { 
    connectionClass: string;
    sourceObject: {
      operation: 'Add' | 'Find';
      type: 'Entity' | 'Video' | 'Image' | 'Frame' | 'Clip' | 'Descriptor';
      class?: string;
      with_class?: string;
      properties?: Record<string, any>;
      constraints?: Record<string, any>;
      uniqueid?: string;
      ref?: number;
      set?: string;
      results?: Record<string, any>;
    };
    targetObject: {
      operation: 'Add' | 'Find';
      type: 'Entity' | 'Video' | 'Image' | 'Frame' | 'Clip' | 'Descriptor';
      class?: string;
      with_class?: string;
      properties?: Record<string, any>;
      constraints?: Record<string, any>;
      uniqueid?: string;
      ref?: number;
      results?: Record<string, any>;
      set?: string;
    };
    connectionProperties?: Record<string, any>;
  }): Promise<{ connection: Connection; sourceObject: any; targetObject: any }> {
    await this.baseClient.ensureAuthenticated();

    const createOperation = (obj: typeof input.sourceObject, defaultRef: number) => {
      const ref = obj.ref || defaultRef;
      
      if (obj.operation === 'Add') {
        const base = {
          "_ref": ref,
          "properties": obj.properties || {}
        };
        return obj.class ? { ...base, "class": obj.class } : base;
      } else { // Find operation
        return {
          "_ref": ref,
          ...(obj.with_class && { "with_class": obj.with_class }),
          ...(obj.class && { "class": obj.class }),
          ...(obj.constraints && { "constraints": obj.constraints }),
          ...(obj.uniqueid && { "uniqueid": obj.uniqueid }),
          "results": {
            "all_properties": true
          }
        };
      }
    };

    const query = [{
      [`${input.sourceObject.operation}${input.sourceObject.type}`]: createOperation(input.sourceObject, 1)
    }, {
      [`${input.targetObject.operation}${input.targetObject.type}`]: createOperation(input.targetObject, 2)
    }, {
      "AddConnection": {
        "class": input.connectionClass,
        "src": 1,
        "dst": 2,
        "properties": input.connectionProperties || {}
      }
    }];

    const [response] = await this.baseClient.query(query, []);
    
    if (!this.isChainedResponse(response) ||
        !response[0][`${input.sourceObject.operation}${input.sourceObject.type}`] ||
        !response[1][`${input.targetObject.operation}${input.targetObject.type}`] ||
        !response[2].AddConnection ||
        response[0][`${input.sourceObject.operation}${input.sourceObject.type}`]?.status !== 0 ||
        response[1][`${input.targetObject.operation}${input.targetObject.type}`]?.status !== 0 ||
        response[2].AddConnection.status !== 0) {
      throw new Error('Failed to create objects and connection');
    }

    const createReturnObject = (obj: typeof input.sourceObject, response: ChainedOperationResponse) => {
      const operationKey = `${obj.operation}${obj.type}`;
      const operationResponse = response[operationKey];
      
      if (obj.operation === 'Find' && operationResponse) {
        // For Find operations, return the found object
        return {
          type: obj.type,
          ...(obj.class && { class: obj.class }),
          ...operationResponse
        };
      } else {
        // For Add operations, construct the object as before
        return {
          type: obj.type,
          ...(obj.class && { class: obj.class }),
          ...(obj.properties || {}),
          created_at: new Date().toISOString(),
          updated_at: new Date().toISOString()
        };
      }
    };

    return {
      sourceObject: createReturnObject(input.sourceObject, response[0]),
      targetObject: createReturnObject(input.targetObject, response[1]),
      connection: {
        class: input.connectionClass,
        src: { ref: 1, ...(input.sourceObject.class && { class: input.sourceObject.class }) },
        dst: { ref: 2, ...(input.targetObject.class && { class: input.targetObject.class }) },
        properties: input.connectionProperties || {},
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      }
    };
  }
} 