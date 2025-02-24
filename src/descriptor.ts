import { BaseClient } from './base.js';
import {
  DescriptorMetadata,
  CreateDescriptorInput,
  FindDescriptorOptions,
  FindDescriptorBatchOptions,
  ClassifyDescriptorOptions,
  QueryOptions
} from './types.js';

export class DescriptorClient {
  private baseClient: BaseClient;

  constructor(baseClient: BaseClient) {
    this.baseClient = baseClient;
  }

  private float32ArrayToBuffer(array: Float32Array): Buffer {
    return Buffer.from(array.buffer);
  }

  private isAddDescriptorResponse(response: unknown): response is [{ AddDescriptor: { status: number } }] {
    return Array.isArray(response) &&
           response.length > 0 &&
           'AddDescriptor' in response[0] &&
           typeof response[0].AddDescriptor === 'object' &&
           response[0].AddDescriptor !== null &&
           'status' in response[0].AddDescriptor;
  }

  private isFindDescriptorResponse(response: unknown): response is [{ FindDescriptor: { entities: DescriptorMetadata[]; returned: number; status: number } }] {
    return Array.isArray(response) &&
           response.length > 0 &&
           'FindDescriptor' in response[0] &&
           typeof response[0].FindDescriptor === 'object' &&
           response[0].FindDescriptor !== null &&
           'entities' in response[0].FindDescriptor &&
           Array.isArray(response[0].FindDescriptor.entities);
  }

  private isFindDescriptorBatchResponse(response: unknown): response is [{ FindDescriptorBatch: { descriptors: DescriptorMetadata[]; returned: number; status: number } }] {
    return Array.isArray(response) &&
           response.length > 0 &&
           'FindDescriptorBatch' in response[0] &&
           typeof response[0].FindDescriptorBatch === 'object' &&
           response[0].FindDescriptorBatch !== null &&
           'descriptors' in response[0].FindDescriptorBatch &&
           Array.isArray(response[0].FindDescriptorBatch.descriptors);
  }

  private isClassifyDescriptorResponse(response: unknown): response is [{ ClassifyDescriptor: { classifications: Record<string, number>; status: number } }] {
    return Array.isArray(response) &&
           response.length > 0 &&
           'ClassifyDescriptor' in response[0] &&
           typeof response[0].ClassifyDescriptor === 'object' &&
           response[0].ClassifyDescriptor !== null &&
           'classifications' in response[0].ClassifyDescriptor &&
           'status' in response[0].ClassifyDescriptor;
  }

  private isDeleteDescriptorResponse(response: unknown): response is [{ DeleteDescriptor: { status: number } }] {
    return Array.isArray(response) &&
           response.length > 0 &&
           'DeleteDescriptor' in response[0] &&
           typeof response[0].DeleteDescriptor === 'object' &&
           response[0].DeleteDescriptor !== null &&
           'status' in response[0].DeleteDescriptor;
  }

  async addDescriptor(input: CreateDescriptorInput): Promise<DescriptorMetadata> {
    await this.baseClient.ensureAuthenticated();

    if (!input.set) {
      throw new Error('set is required for addDescriptor');
    }

    const query = [{
      "AddDescriptor": {
        "label": input.label,
        "properties": input.properties || {},
        "set": input.set,
        "_ref": 1
      }
    }];

    const [response] = await this.baseClient.query(query, [this.float32ArrayToBuffer(input.blob)]);
    if (!this.isAddDescriptorResponse(response)) {
      throw new Error(`Invalid response from server : ${JSON.stringify(response)}`);
    }

    const metadata: DescriptorMetadata = {
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    };

    if (input.label) {
      metadata.label = input.label;
    }

    if (input.properties) {
      metadata.properties = input.properties;
    }

    return metadata;
  }

  async findDescriptors(descriptor?: Float32Array, options?: FindDescriptorOptions & QueryOptions): Promise<DescriptorMetadata[]> {
    await this.baseClient.ensureAuthenticated();

    if (descriptor && !options?.set) {
      throw new Error('set is required for k-nearest neighbor search');
    }

    const query = [{
      "FindDescriptor": {
        "constraints": options?.constraints,
        "results": {
          "all_properties": true,
          ...options?.results
        },
        "set": options?.set,
        "uniqueids": options?.uniqueids,
        "k_neighbors": descriptor ? (options?.k_neighbors || 10) : undefined,
        "knn_first": options?.knn_first,
        "engine": options?.engine,
        "metric": options?.metric,
        "labels": options?.labels,
        "distances": options?.distances,
        "blobs": options?.blobs,
        "with_label": options?.with_label,
        "indexed_results_only": options?.indexed_results_only,
        "limit": options?.limit,
        "offset": options?.offset,
        "sort": options?.sort,
        "_ref": descriptor ? 1 : undefined
      }
    }];

    const [response] = await this.baseClient.query(query, descriptor ? [this.float32ArrayToBuffer(descriptor)] : []);
    if (!this.isFindDescriptorResponse(response)) {
      return [];
    }

    return response[0].FindDescriptor.entities;
  }

  async findDescriptor(options?: FindDescriptorOptions): Promise<DescriptorMetadata> {
    const descriptors = await this.findDescriptors(undefined, { ...options, limit: 1 });
    if (!descriptors.length) {
      throw new Error('No descriptors found');
    }
    return descriptors[0];
  }

  async findDescriptorBatch(descriptors: Float32Array[], options?: FindDescriptorBatchOptions & QueryOptions): Promise<DescriptorMetadata[]> {
    await this.baseClient.ensureAuthenticated();

    if (!options?.set) {
      throw new Error('set is required for findDescriptorBatch');
    }

    const query = [{
      "FindDescriptorBatch": {
        "constraints": options?.constraints,
        "results": {
          "all_properties": true,
          ...options?.results
        },
        "set": options.set,
        "uniqueids": options?.uniqueids,
        "limit": options?.limit,
        "offset": options?.offset,
        "sort": options?.sort
      }
    }];

    const blobs = descriptors.map(d => this.float32ArrayToBuffer(d));
    const [response] = await this.baseClient.query(query, blobs);
    if (!this.isFindDescriptorBatchResponse(response)) {
      return [];
    }

    return response[0].FindDescriptorBatch.descriptors;
  }

  async classifyDescriptor(descriptor: Float32Array, options: ClassifyDescriptorOptions): Promise<Record<string, number>> {
    await this.baseClient.ensureAuthenticated();

    if (!options.set) {
      throw new Error('set is required for classifyDescriptor');
    }

    const query = [{
      "ClassifyDescriptor": {
        "set": options.set,
        "k": options.k,
        "threshold": options.threshold
      }
    }];

    const [response] = await this.baseClient.query(query, [this.float32ArrayToBuffer(descriptor)]);
    if (!this.isClassifyDescriptorResponse(response)) {
      throw new Error(`Invalid response from server : ${JSON.stringify(response)}`);
    }

    return response[0].ClassifyDescriptor.classifications;
  }

  async deleteDescriptor(options: { set?: string; constraints?: Record<string, any>; uniqueids?: string[] } = {}): Promise<void> {
    await this.baseClient.ensureAuthenticated();

    const query = [{
      "DeleteDescriptor": {
        "set": options.set,
        "constraints": options.constraints,
      }
    }];

    const [response] = await this.baseClient.query(query, []);
    if (!this.isDeleteDescriptorResponse(response)) {
      throw new Error(`Invalid response from server : ${JSON.stringify(response)}`);
    }
  }
}