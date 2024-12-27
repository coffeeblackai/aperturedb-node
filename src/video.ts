import { BaseClient } from './base.js';
import {
  VideoMetadata,
  CreateVideoInput,
  FindVideoOptions,
  QueryOptions
} from './types.js';

export class VideoClient {
  private baseClient: BaseClient;

  constructor(baseClient: BaseClient) {
    this.baseClient = baseClient;
  }

  async addVideo(input: CreateVideoInput): Promise<VideoMetadata> {
    await this.baseClient.ensureAuthenticated();
    
    if (!input.url && !input.blob) {
      throw new Error('Either url or blob is required for addVideo');
    }

    const query = [{
      "AddVideo": {
        "url": input.url,
        "properties": input.properties || {}
      }
    }];

    type AddVideoResponse = [{
      AddVideo: {
        status: number;
      }
    }];

    const blobs = input.blob ? [input.blob] : [];
    await this.baseClient.query<AddVideoResponse>(query, blobs);
    
    return {
      ...input.properties || {},
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    };
  }

  async findVideo(options?: FindVideoOptions): Promise<VideoMetadata> {
    await this.baseClient.ensureAuthenticated();
    
    const query = [{
      "FindVideo": {
        "constraints": options?.constraints,
        "results": {
          "all_properties": true,
          ...options?.results
        },
        "uniqueids": options?.uniqueids,
        "blobs": options?.blobs
      }
    }];

    type FindVideoResponse = [{
      FindVideo: {
        entities: VideoMetadata[];
        returned: number;
        status: number;
        blobs?: Buffer[];
      }
    }];

    const [response, blobs] = await this.baseClient.query<FindVideoResponse>(query, []);
    const video = response[0].FindVideo.entities[0];
    if (options?.blobs && blobs.length > 0) {
      video._blob = blobs[0];
    }
    return video;
  }

  async findVideos(options?: FindVideoOptions & QueryOptions): Promise<VideoMetadata[]> {
    await this.baseClient.ensureAuthenticated();
    
    const query = [{
      "FindVideo": {
        "constraints": options?.constraints,
        "results": {
          "all_properties": true,
          ...options?.results
        },
        "uniqueids": options?.uniqueids,
        "limit": options?.limit,
        "offset": options?.offset,
        "sort": options?.sort,
        "blobs": options?.blobs
      }
    }];

    type FindVideoResponse = [{
      FindVideo: {
        entities: VideoMetadata[];
        returned: number;
        status: number;
        blobs?: Buffer[];
      }
    }];

    const [response, blobs] = await this.baseClient.query<FindVideoResponse>(query, []);
    const videos = response[0].FindVideo.entities || [];
    
    if (options?.blobs && blobs.length > 0) {
      videos.forEach((video, index) => {
        if (index < blobs.length) {
          video._blob = blobs[index];
        }
      });
    }
    
    return videos;
  }

  async deleteVideo(constraints: Record<string, any>): Promise<void> {
    await this.baseClient.ensureAuthenticated();
    
    const query = [{
      "DeleteVideo": {
        "constraints": constraints
      }
    }];

    type DeleteVideoResponse = [{
      DeleteVideo: {
        status: number;
      }
    }];

    await this.baseClient.query<DeleteVideoResponse>(query, []);
  }
} 