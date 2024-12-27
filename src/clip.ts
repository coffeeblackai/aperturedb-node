import { BaseClient } from './base.js';
import {
  ClipMetadata,
  CreateClipInput,
  UpdateClipInput,
  FindClipOptions,
  QueryOptions
} from './types.js';
import { Logger } from './utils/logger.js';

export class ClipClient {
  private baseClient: BaseClient;

  constructor(baseClient: BaseClient) {
    this.baseClient = baseClient;
  }

  private isChainedAddClipResponse(response: unknown): response is [
    { FindVideo: { returned: number; status: number } },
    { AddClip: { status: number } }
  ] {
    if (!Array.isArray(response)) {
      Logger.error('Response is not an array:', response);
      throw new Error('Invalid response format');
    }
    
    if (response.length !== 2) {
      Logger.error('Response array length is not 2:', response.length);
      throw new Error('Invalid response length');
    }
    
    if (!('FindVideo' in response[0])) {
      Logger.error('First element missing FindVideo:', response[0]);
      throw new Error('Invalid response format');
    }
    
    if (!('AddClip' in response[1])) {
      Logger.error('Second element missing AddClip:', response[1]);
      throw new Error('Invalid response format');
    }

    const findVideo = response[0].FindVideo;
    const addClip = response[1].AddClip;
    
    if (typeof findVideo !== 'object' || findVideo === null) {
      Logger.error('FindVideo is not an object:', findVideo);
      throw new Error('Invalid FindVideo format');
    }
    
    if (typeof addClip !== 'object' || addClip === null) {
      Logger.error('AddClip is not an object:', addClip);
      throw new Error('Invalid AddClip format');
    }
    
    if (!('status' in findVideo) || !('entities' in findVideo)) {
      Logger.error('FindVideo missing required properties:', findVideo);
      throw new Error('Invalid FindVideo format');
    }
    
    if (!('status' in addClip)) {
      Logger.error('AddClip missing status:', addClip);
      throw new Error('Invalid AddClip format');
    }

    return true;
  }

  private isFindClipResponse(response: unknown): response is { FindClip: { entities: ClipMetadata[]; returned: number; status: number } }[] {
    return Array.isArray(response) && 
           response.length > 0 && 
           'FindClip' in response[0] &&
           typeof response[0].FindClip === 'object' &&
           response[0].FindClip !== null &&
           'entities' in response[0].FindClip &&
           Array.isArray(response[0].FindClip.entities);
  }

  private isUpdateClipResponse(response: unknown): response is { UpdateClip: { status: number } }[] {
    return Array.isArray(response) && 
           response.length > 0 && 
           'UpdateClip' in response[0] &&
           typeof response[0].UpdateClip === 'object' &&
           response[0].UpdateClip !== null &&
           'status' in response[0].UpdateClip;
  }

  private isDeleteClipResponse(response: unknown): response is { DeleteClip: { status: number } }[] {
    return Array.isArray(response) && 
           response.length > 0 && 
           'DeleteClip' in response[0] &&
           typeof response[0].DeleteClip === 'object' &&
           response[0].DeleteClip !== null &&
           'status' in response[0].DeleteClip;
  }

  async addClip(input: CreateClipInput): Promise<ClipMetadata> {
    await this.baseClient.ensureAuthenticated();
    
    if (!input.video_ref) {
      throw new Error('video_ref is required for addClip');
    }

    // Validate that at least one range parameter is provided
    if (!input.frame_number_range && !input.time_offset_range && !input.time_fraction_range) {
      throw new Error('One of frame_number_range, time_offset_range, or time_fraction_range must be provided');
    }

    const query = [{
      "FindVideo": {
        "_ref": 1,
        "unique": true,
        "blobs": false,
        "constraints": {
          "_uniqueid": ["==", input.video_ref]
        }
      }
    }, {
      "AddClip": {
        "video_ref": 1,
        "frame_number_range": input.frame_number_range,
        "time_offset_range": input.time_offset_range,
        "time_fraction_range": input.time_fraction_range,
        "label": input.label,
        "properties": input.properties || {}
      }
    }];

    const [response] = await this.baseClient.query(query, []);
    if (!this.isChainedAddClipResponse(response)) {
      throw new Error('Invalid response from server');
    }

    if (response[0].FindVideo.status !== 0 || response[1].AddClip.status !== 0) {
      throw new Error('Failed to create clip');
    }

    return {
      video_ref: input.video_ref,
      frame_number_range: input.frame_number_range,
      time_offset_range: input.time_offset_range,
      time_fraction_range: input.time_fraction_range,
      label: input.label,
      ...input.properties,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    };
  }

  async findClip(options?: FindClipOptions): Promise<ClipMetadata | undefined> {
    await this.baseClient.ensureAuthenticated();
    
    const query = [{
      "FindClip": {
        "video_ref": options?.video_ref,
        "constraints": options?.constraints,
        "results": {
          "all_properties": true,
          ...options?.results
        },
        "uniqueids": options?.uniqueids,
      }
    }];

    const [response] = await this.baseClient.query(query, []);
    if (!this.isFindClipResponse(response) || !response[0].FindClip.entities.length) {
      return undefined;
    }

    return response[0].FindClip.entities[0];
  }

  async findClips(options?: FindClipOptions & QueryOptions): Promise<ClipMetadata[]> {
    await this.baseClient.ensureAuthenticated();
    
    const query = [{
      "FindClip": {
        "video_ref": options?.video_ref,
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
    if (!this.isFindClipResponse(response)) {
      return [];
    }

    return response[0].FindClip.entities;
  }

  async findClipsByVideoConstraints(
    videoConstraints: Record<string, [string, any]>,
    options?: Omit<FindClipOptions & QueryOptions, 'video_ref'> & {
      videoResults?: {
        all_properties?: boolean;
        properties?: string[];
      };
    }
  ): Promise<ClipMetadata[]> {
    await this.baseClient.ensureAuthenticated();
    
    const query = [{
      "FindVideo": {
        "_ref": 1,
        "unique": true,
        "blobs": false,
        "constraints": videoConstraints,
        "results": {
          "all_properties": true,
          ...options?.videoResults
        }
      }
    }, {
      "FindClip": {
        "video_ref": 1,
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
    if (!Array.isArray(response) || response.length !== 2) {
      return [];
    }

    const findVideo = response[0].FindVideo;
    const findClip = response[1].FindClip;

    if (!findVideo || !findClip || findVideo.status !== 0 || findClip.status !== 0) {
      return [];
    }

    return findClip.entities || [];
  }

  // Helper method to maintain backward compatibility
  async findClipsByVideo(videoName: string, options?: Omit<FindClipOptions & QueryOptions, 'video_ref'>): Promise<ClipMetadata[]> {
    return this.findClipsByVideoConstraints(
      { name: ["==", videoName] },
      options
    );
  }

  async updateClip(options: UpdateClipInput): Promise<void> {
    await this.baseClient.ensureAuthenticated();
    
    const query = [{
      "UpdateClip": {
        "properties": options.properties,
        "remove_props": options.remove_props,
        "constraints": options.constraints
      }
    }];

    const [response] = await this.baseClient.query(query, []);
    if (!this.isUpdateClipResponse(response)) {
      throw new Error('Invalid response from server');
    }
  }

  async deleteClip(constraints: Record<string, any>): Promise<void> {
    await this.baseClient.ensureAuthenticated();
    
    const query = [{
      "DeleteClip": {
        "constraints": constraints
      }
    }];

    const [response] = await this.baseClient.query(query, []);
    if (!this.isDeleteClipResponse(response)) {
      throw new Error('Invalid response from server');
    }
  }
} 