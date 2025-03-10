import { BaseClient } from './base.js';
import {
  FrameMetadata,
  CreateFrameInput,
  UpdateFrameInput,
  FindFrameOptions,
  QueryOptions
} from './types.js';
import { Logger } from './utils/logger.js';

export class FrameClient {
  private baseClient: BaseClient;

  constructor(baseClient: BaseClient) {
    this.baseClient = baseClient;
  }

  private isChainedAddFrameResponse(response: unknown): response is [
    { FindVideo: { returned: number; status: number } },
    { AddFrame: { status: number } }
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
      Logger.trace('Debug - response[0]:', response[0]);
      Logger.trace('Debug - FindVideo in response[0]:', 'FindVideo' in response[0]);
      Logger.trace('Debug - typeof response[0]:', typeof response[0]);
      Logger.error('First element missing FindVideo:', response[0]);
      throw new Error('Invalid response format');
    }

    if (!('AddFrame' in response[1])) {
      Logger.error('Second element missing AddFrame:', response[1]);
      throw new Error('Invalid response format');
    }

    const findVideo = response[0].FindVideo;
    const addFrame = response[1].AddFrame;

    if (typeof findVideo !== 'object' || findVideo === null) {
      Logger.error('FindVideo is not an object:', findVideo);
      throw new Error('Invalid FindVideo format');
    }

    if (typeof addFrame !== 'object' || addFrame === null) {
      Logger.error('AddFrame is not an object:', addFrame);
      throw new Error('Invalid AddFrame format');
    }


    return true;
  }

  private isFindFrameResponse(response: unknown): response is { FindFrame: { entities: FrameMetadata[]; returned: number; status: number } }[] {
    return Array.isArray(response) &&
           response.length > 0 &&
           'FindFrame' in response[0] &&
           typeof response[0].FindFrame === 'object' &&
           response[0].FindFrame !== null &&
           'entities' in response[0].FindFrame &&
           Array.isArray(response[0].FindFrame.entities);
  }

  private isUpdateFrameResponse(response: unknown): response is { UpdateFrame: { status: number } }[] {
    return Array.isArray(response) &&
           response.length > 0 &&
           'UpdateFrame' in response[0] &&
           typeof response[0].UpdateFrame === 'object' &&
           response[0].UpdateFrame !== null &&
           'status' in response[0].UpdateFrame;
  }

  private isDeleteFrameResponse(response: unknown): response is { DeleteFrame: { status: number } }[] {
    return Array.isArray(response) &&
           response.length > 0 &&
           'DeleteFrame' in response[0] &&
           typeof response[0].DeleteFrame === 'object' &&
           response[0].DeleteFrame !== null &&
           'status' in response[0].DeleteFrame;
  }

  async addFrame(input: CreateFrameInput): Promise<FrameMetadata> {
    await this.baseClient.ensureAuthenticated();

    if (!input.video_ref && !input.constraints) {
      throw new Error('Either video_ref or constraints must be provided');
    }

    // Validate that at least one position parameter is provided
    if (!input.frame_number && !input.time_offset && !input.time_fraction) {
      throw new Error('One of frame_number, time_offset, or time_fraction must be provided');
    }

    const query = [{
      "FindVideo": {
        "_ref": 1,
        "unique": true,
        "blobs": false,
        "constraints": input.constraints
      }
    }, {
      "AddFrame": {
        "video_ref": 1,
        "frame_number": input.frame_number,
        "time_offset": input.time_offset,
        "time_fraction": input.time_fraction,
        "label": input.label,
        "properties": input.properties || {}
      }
    }];

    const [response] = await this.baseClient.query(query, []);
    if (!this.isChainedAddFrameResponse(response)) {
      throw new Error(`Invalid response from server : ${JSON.stringify(response)}`);
    }

    if (response[0].FindVideo.status !== 0 || response[1].AddFrame.status !== 0) {
      throw new Error('Failed to create frame');
    }

    return {
      video_ref: input.video_ref || '',
      frame_number: input.frame_number,
      time_offset: input.time_offset,
      time_fraction: input.time_fraction,
      ...input.properties || {},
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    };
  }

  async findFrame(options?: FindFrameOptions): Promise<FrameMetadata> {
    await this.baseClient.ensureAuthenticated();

    const query = [{
      "FindFrame": {
        "video_ref": options?.video_ref,
        "constraints": options?.constraints,
        "results": {
          "all_properties": true,
          ...options?.results
        },
        "uniqueids": options?.uniqueids,
        "operations": options?.operations,
        "in_frame_number_range": options?.in_frame_number_range,
        "in_time_offset_range": options?.in_time_offset_range,
        "in_time_fraction_range": options?.in_time_fraction_range,
        "frame_numbers": options?.frame_numbers,
        "time_offsets": options?.time_offsets,
        "time_fractions": options?.time_fractions,
        "labels": options?.labels,
        "as_format": options?.as_format,
        "with_label": options?.with_label
      }
    }];

    const [response] = await this.baseClient.query(query, []);
    if (!this.isFindFrameResponse(response) || !response[0].FindFrame.entities.length) {
      throw new Error('No frames found');
    }

    return response[0].FindFrame.entities[0];
  }

  async findFrames(options?: FindFrameOptions & QueryOptions): Promise<FrameMetadata[]> {
    await this.baseClient.ensureAuthenticated();

    const query = [{
      "FindFrame": {
        "video_ref": options?.video_ref,
        "constraints": options?.constraints,
        "results": {
          "all_properties": true,
          ...options?.results
        },
        "uniqueids": options?.uniqueids,
        "operations": options?.operations,
        "in_frame_number_range": options?.in_frame_number_range,
        "in_time_offset_range": options?.in_time_offset_range,
        "in_time_fraction_range": options?.in_time_fraction_range,
        "frame_numbers": options?.frame_numbers,
        "time_offsets": options?.time_offsets,
        "time_fractions": options?.time_fractions,
        "labels": options?.labels,
        "as_format": options?.as_format,
        "with_label": options?.with_label,
        "limit": options?.limit,
        "offset": options?.offset,
        "sort": options?.sort
      }
    }];

    const [response] = await this.baseClient.query(query, []);
    if (!this.isFindFrameResponse(response)) {
      return [];
    }

    return response[0].FindFrame.entities;
  }

  async findFramesByVideoConstraints(
    videoConstraints: Record<string, [string, any]>,
    options?: Omit<FindFrameOptions & QueryOptions, 'video_ref'> & {
      videoResults?: {
        all_properties?: boolean;
        properties?: string[];
      };
    }
  ): Promise<FrameMetadata[]> {
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
      "FindFrame": {
        "video_ref": 1,
        "constraints": options?.constraints,
        "results": {
          "all_properties": true,
          ...options?.results
        },
        "uniqueids": options?.uniqueids,
        "operations": options?.operations,
        "in_frame_number_range": options?.in_frame_number_range,
        "in_time_offset_range": options?.in_time_offset_range,
        "in_time_fraction_range": options?.in_time_fraction_range,
        "frame_numbers": options?.frame_numbers,
        "time_offsets": options?.time_offsets,
        "time_fractions": options?.time_fractions,
        "labels": options?.labels,
        "as_format": options?.as_format,
        "with_label": options?.with_label,
        "limit": options?.limit,
        "offset": options?.offset,
        "sort": options?.sort
      }
    }];

    const [response] = await this.baseClient.query(query, []);

    // Check if response is an array with two elements
    if (!Array.isArray(response) || response.length !== 2) {
      return [];
    }

    // Check if both FindVideo and FindFrame exist in the response
    if (!('FindVideo' in response[0]) || !('FindFrame' in response[1])) {
      return [];
    }

    const findVideo = response[0].FindVideo;
    const findFrame = response[1].FindFrame;

    // Check if both operations were successful and have valid status
    if (!findVideo || !findFrame || findVideo.status !== 0 || findFrame.status !== 0) {
      return [];
    }

    // Return the frame entities if they exist
    return findFrame.entities || [];
  }

  // Helper method to maintain backward compatibility
  async findFramesByVideo(videoName: string, options?: Omit<FindFrameOptions & QueryOptions, 'video_ref'>): Promise<FrameMetadata[]> {
    return this.findFramesByVideoConstraints(
      { name: ["==", videoName] },
      options
    );
  }

  async updateFrame(options: UpdateFrameInput): Promise<void> {
    await this.baseClient.ensureAuthenticated();

    const query = [{
      "UpdateFrame": {
        "properties": options.properties,
        "remove_props": options.remove_props,
        "constraints": options.constraints
      }
    }];

    const [response] = await this.baseClient.query(query, []);
    if (!this.isUpdateFrameResponse(response)) {
      throw new Error(`Invalid response from server : ${JSON.stringify(response)}`);
    }
  }

  async deleteFrame(constraints: Record<string, any>): Promise<void> {
    await this.baseClient.ensureAuthenticated();

    const query = [{
      "DeleteFrame": {
        "constraints": constraints
      }
    }];

    const [response] = await this.baseClient.query(query, []);
    if (!this.isDeleteFrameResponse(response)) {
      throw new Error(`Invalid response from server : ${JSON.stringify(response)}`);
    }
  }
}