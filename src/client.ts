import { BaseClient } from './base.js';
import { EntityClient } from './entity.js';
import { VideoClient } from './video.js';
import { ImageClient } from './image.js';
import { FrameClient } from './frame.js';
import { ClipClient } from './clip.js';
import { DescriptorClient } from './descriptor.js';
import { DescriptorSetClient } from './descriptor_set.js';
import { ConnectionClient } from './connection.js';
import { BoundingBoxClient } from './bbox.js';
import { PolygonClient } from './polygon.js';
import type { ApertureConfig } from './types.js';
import type { QueryExecutor } from './parallel.js';

export class ApertureClient extends BaseClient implements QueryExecutor {
  private static instance: ApertureClient;
  private _entities: EntityClient;
  private _videos: VideoClient;
  private _images: ImageClient;
  private _frames: FrameClient;
  private _clips: ClipClient;
  private _descriptors: DescriptorClient;
  private _descriptorSets: DescriptorSetClient;
  private _connections: ConnectionClient;
  private _boundingBoxes: BoundingBoxClient;
  private _polygons: PolygonClient;

  constructor(config?: Partial<ApertureConfig>) {
    super(config);
    this._entities = new EntityClient(this);
    this._videos = new VideoClient(this);
    this._images = new ImageClient(this);
    this._frames = new FrameClient(this);
    this._clips = new ClipClient(this);
    this._descriptors = new DescriptorClient(this);
    this._descriptorSets = new DescriptorSetClient(this);
    this._connections = new ConnectionClient(this);
    this._boundingBoxes = new BoundingBoxClient(this);
    this._polygons = new PolygonClient(this);
  }

  static getInstance(config?: Partial<ApertureConfig>): ApertureClient {
    if (!ApertureClient.instance) {
      ApertureClient.instance = new ApertureClient(config);
    }
    return ApertureClient.instance;
  }

  get entities(): EntityClient {
    return this._entities;
  }

  get videos(): VideoClient {
    return this._videos;
  }

  get images(): ImageClient {
    return this._images;
  }

  get frames(): FrameClient {
    return this._frames;
  }

  get clips(): ClipClient {
    return this._clips;
  }

  get descriptors(): DescriptorClient {
    return this._descriptors;
  }

  get descriptorSets(): DescriptorSetClient {
    return this._descriptorSets;
  }

  get connections(): ConnectionClient {
    return this._connections;
  }

  get boundingBoxes(): BoundingBoxClient {
    return this._boundingBoxes;
  }

  get polygons(): PolygonClient {
    return this._polygons;
  }

  async rawQuery<T = any>(query: any, blobs: Buffer[] = []): Promise<[T, Buffer[]]> {
    return this.query<T>(query, blobs);
  }
} 