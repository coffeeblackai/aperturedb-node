import { BaseClient } from './base';
import { ImageClient } from './image';
import { DescriptorClient } from './descriptor';
import { DescriptorSetClient } from './descriptor_set';
import { PolygonClient } from './polygon';
import { BoundingBoxClient } from './bbox';
import { VideoClient } from './video';
import { FrameClient } from './frame';
import { ClipClient } from './clip';
import { EntityClient } from './entity';
import { ConnectionClient } from './connection';
import type { ApertureConfig } from './types';

export class ApertureClient {
  private static instance: ApertureClient | null = null;
  private baseClient: BaseClient;

  public readonly images: ImageClient;
  public readonly descriptors: DescriptorClient;
  public readonly descriptorSets: DescriptorSetClient;
  public readonly polygons: PolygonClient;
  public readonly boundingBoxes: BoundingBoxClient;
  public readonly videos: VideoClient;
  public readonly frames: FrameClient;
  public readonly clips: ClipClient;
  public readonly entities: EntityClient;
  public readonly connections: ConnectionClient;

  private constructor(config?: Partial<ApertureConfig>) {
    // Create base client first
    this.baseClient = new BaseClient(config);

    // Share the same base client instance across all clients
    this.images = new ImageClient(this.baseClient);
    this.descriptors = new DescriptorClient(this.baseClient);
    this.descriptorSets = new DescriptorSetClient(this.baseClient);
    this.polygons = new PolygonClient(this.baseClient);
    this.boundingBoxes = new BoundingBoxClient(this.baseClient);
    this.videos = new VideoClient(this.baseClient);
    this.frames = new FrameClient(this.baseClient);
    this.clips = new ClipClient(this.baseClient);
    this.entities = new EntityClient(this.baseClient);
    this.connections = new ConnectionClient(this.baseClient);

    console.debug('Created new ApertureDB client instance');
  }

  public static getInstance(config?: Partial<ApertureConfig>): ApertureClient {
    if (!ApertureClient.instance) {
      ApertureClient.instance = new ApertureClient(config);
    }
    return ApertureClient.instance;
  }

  // For testing - allows resetting the singleton
  public static _reset(): void {
    ApertureClient.instance = null;
  }

  /**
   * Execute a raw query against ApertureDB.
   * This method allows sending any valid query format without type restrictions.
   * Use this for experimental features or endpoints not yet supported in the typed API.
   * 
   * @param query - The query object or array to send
   * @param blobs - Optional array of binary blobs to send with the query
   * @returns A tuple of [response, blobs] where response is the raw server response
   */
  public async rawQuery<T = any>(query: any, blobs: Buffer[] = []): Promise<[T, Buffer[]]> {
    return this.baseClient.query<T>(query, blobs);
  }
}

// Export a function to get the singleton instance
export function getClient(config?: Partial<ApertureConfig>): ApertureClient {
  return ApertureClient.getInstance(config);
}

// Export the client types for type information
export { BaseClient } from './base';
export { ImageClient } from './image';
export { DescriptorClient } from './descriptor';
export { DescriptorSetClient } from './descriptor_set';
export { PolygonClient } from './polygon';
export { BoundingBoxClient } from './bbox';
export { VideoClient } from './video';
export { FrameClient } from './frame';
export { ClipClient } from './clip';
export { EntityClient } from './entity';
export { ConnectionClient } from './connection'; 