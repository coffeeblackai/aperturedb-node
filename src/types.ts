// Import client types
import type { ImageClient } from './image.js';
import type { DescriptorClient } from './descriptor.js';
import type { DescriptorSetClient } from './descriptor_set.js';
import type { PolygonClient } from './polygon.js';
import type { BoundingBoxClient } from './bbox.js';
import type { VideoClient } from './video.js';
import type { FrameClient } from './frame.js';
import type { ClipClient } from './clip.js';
import type { EntityClient } from './entity.js';
import type { ConnectionClient } from './connection.js';
import type { LogLevel } from './utils/logger.js';

// Core types
export interface ApertureConfig {
  host: string;
  port?: number;
  username: string;
  password: string;
  token?: string;
  useSsl?: boolean;
  useKeepalive?: boolean;
  retryIntervalSeconds?: number;
  retryMaxAttempts?: number;
  apiUrl?: string;
}

// Client type
export interface ApertureClient {
  readonly images: ImageClient;
  readonly descriptors: DescriptorClient;
  readonly descriptorSets: DescriptorSetClient;
  readonly polygons: PolygonClient;
  readonly boundingBoxes: BoundingBoxClient;
  readonly videos: VideoClient;
  readonly frames: FrameClient;
  readonly clips: ClipClient;
  readonly entities: EntityClient;
  readonly connections: ConnectionClient;
  setLogLevel(level: LogLevel): void;
}

export interface AuthResponse {
  json: {
    Authenticate: {
      session_token: string;
      refresh_token: string;
      session_token_expires_in: number;
      refresh_token_expires_in: number;
      status: number;
      info?: string;
    }
  }[];
}

export interface QueryResponse<T> {
  json: T[];
  blobs: Blob[];
}

// Error handling
export class ApertureError extends Error {
  constructor(
    message: string,
    public code: string,
    public status: number,
    public originalError?: any
  ) {
    super(message);
    this.name = 'ApertureError';
  }
}

// Query options
export interface QueryOptions {
  limit?: number;
  offset?: number;
  sort?: {
    field: string;
    order: 'asc' | 'desc';
  };
}

// Entity types
export interface EntityResponse {
  class: string;
  _uniqueid?: string;  // Only present in find results
  [key: string]: any;  // Allow any properties at the base level
}

export type Entity = EntityResponse;

// Descriptor types
export interface DescriptorMetadata {
  label?: string;
  created_at?: string;
  updated_at?: string;
  [key: string]: any;  // Allow any properties at the base level
}

export interface CreateDescriptorInput {
  set: string;
  blob: Float32Array;
  label?: string;
  properties?: Record<string, any>;
}

export interface FindDescriptorOptions {
  set?: string;
  k_neighbors?: number;
  knn_first?: boolean;
  engine?: 'HNSW' | 'Flat';
  metric?: 'L2' | 'IP' | 'COSINE';
  labels?: boolean;
  distances?: boolean;
  blobs?: boolean;
  with_label?: string;
  indexed_results_only?: boolean;
  constraints?: Record<string, any>;
  results?: Record<string, any>;
  uniqueids?: string[];
}

export interface FindDescriptorBatchOptions {
  set: string;
  constraints?: Record<string, any>;
  results?: Record<string, any>;
  uniqueids?: string[];
}

export interface ClassifyDescriptorOptions {
  set: string;
  k: number;
  threshold?: number;
}

// Descriptor Set types
export interface DescriptorSetResponse extends DescriptorSet {
  created_at: string;
  updated_at: string;
  _uniqueid?: string;
}

export interface DescriptorSet {
  name: string;
  dimensions: number;
  metric: 'L2' | 'IP' | 'COSINE';
  engine: 'HNSW' | 'Flat';
  [key: string]: any;  // Allow any properties at the base level
}

// Image types
export interface ImageMetadata {
  url?: string;
  created_at: string;
  updated_at: string;
  _uniqueid?: string;
  [key: string]: any;  // Allow any properties at the base level
}

export interface CreateImageInput {
  url?: string;
  blob?: Buffer;
  properties?: Record<string, any>;
  descriptor?: Float32Array;
  descriptorSet?: string;
  format?: 'jpg' | 'png';
  operations?: ImageOperation[];
}

export interface FindImageOptions {
  constraints?: Record<string, any>;
  results?: Record<string, any>;
  uniqueids?: string[];
}

export interface ImageOperation {
  type: 'resize' | 'rotate' | 'threshold';
  width?: number;
  height?: number;
  angle?: number;
  value?: number;
  resize?: boolean;
}

// Video types
export interface VideoMetadata {
  created_at: string;
  updated_at: string;
  _fps?: number;
  _frame_count?: number;
  _frame_height?: number;
  _frame_width?: number;
  _duration_us?: number;
  _uniqueid?: string;
  _blob?: Buffer;  // Add blob field for video data when requested
  [key: string]: any;  // Allow any properties at the base level
}

export interface VideoOperation {
  type: 'resize' | 'interval';
  width?: number;
  height?: number;
  start?: number;
  stop?: number;
  step?: number;
}

export interface CreateVideoInput {
  url?: string;
  codec?: string;
  container?: string;
  properties?: Record<string, any>;
  operations?: VideoOperation[];
  blob?: Buffer;
}

export interface UpdateVideoInput {
  ref?: number;
  codec?: string;
  properties?: Record<string, any>;
  remove_props?: string[];
  operations?: VideoOperation[];
}

export interface FindVideoOptions {
  blobs?: boolean;
  urls?: boolean;
  uniqueids?: boolean;
  as_codec?: string;
  as_container?: string;
  with_url?: string;
  operations?: VideoOperation[];
  constraints?: Record<string, [string, any]>;
  results?: {
    all_properties?: boolean;
    properties?: string[];
    [key: string]: any;
  };
}

// Frame types
export interface FrameMetadata {
  video_ref: string;
  frame_number?: number;
  time_offset?: string;
  time_fraction?: number;
  created_at: string;
  updated_at: string;
  _uniqueid?: string;
  label?: string;
  [key: string]: any;  // Allow any properties at the base level
}

export interface CreateFrameInput {
  video_ref: string;
  frame_number?: number;
  time_offset?: string;
  time_fraction?: number;
  properties?: Record<string, any>;
  label?: string;
}

export interface UpdateFrameInput {
  properties?: Record<string, any>;
  remove_props?: string[];
  constraints?: Record<string, [string, any]>;
}

export interface FindFrameOptions {
  video_ref?: number;
  blobs?: boolean;
  operations?: VideoOperation[];
  in_frame_number_range?: FrameNumberRange;
  in_time_offset_range?: TimeRange;
  in_time_fraction_range?: TimeFractionRange;
  frame_numbers?: boolean;
  time_offsets?: boolean;
  time_fractions?: boolean;
  labels?: boolean;
  uniqueids?: boolean;
  as_format?: 'jpg' | 'png';
  with_label?: string;
  constraints?: Record<string, [string, any]>;
  results?: {
    all_properties?: boolean;
    properties?: string[];
    [key: string]: any;
  };
}

export interface TimeRange {
  start: string;
  stop: string;
}

export interface FrameNumberRange {
  start: number;
  stop?: number;
}

export interface TimeFractionRange {
  start: number;
  stop: number;
}

// Clip types
export interface ClipMetadata {
  video_ref: string;
  frame_number_range?: FrameNumberRange;
  time_offset_range?: TimeRange;
  time_fraction_range?: TimeFractionRange;
  label?: string;
  created_at: string;
  updated_at: string;
  _uniqueid?: string;
  [key: string]: any;  // Allow any properties at the base level
}

export interface CreateClipInput {
  video_ref: string;
  frame_number_range?: FrameNumberRange;
  time_offset_range?: TimeRange;
  time_fraction_range?: TimeFractionRange;
  label?: string;
  properties?: Record<string, any>;
}

export interface FindClipOptions {
  video_ref?: string;
  constraints?: Record<string, any>;
  results?: {
    all_properties?: boolean;
    properties?: string[];
    [key: string]: any;
  };
  uniqueids?: boolean;
}

export interface UpdateClipInput {
  properties?: Record<string, any>;
  remove_props?: string[];
  constraints?: Record<string, any>;
}

// Bounding Box types
export interface BoundingBoxMetadata {
  image_ref?: string;
  x: number;
  y: number;
  width: number;
  height: number;
  created_at?: string;
  updated_at?: string;
  _uniqueid?: string;
  [key: string]: any; 
}

export interface CreateBoundingBoxInput {
  image_ref?: string;
  x: number;
  y: number;
  width: number;
  height: number;
  properties?: Record<string, any>;
}

export interface FindBoundingBoxOptions {
  constraints?: Record<string, any>;
  results?: Record<string, any>;
  uniqueids?: string[];
}

// Polygon types
export interface PolygonMetadata {
  image_ref?: string;
  points: [number, number][];
  created_at?: string;
  updated_at?: string;
  _uniqueid?: string;
  [key: string]: any;  // Allow any properties at the base level
}

export interface CreatePolygonInput {
  image_ref: string;
  points: [number, number][];
  properties?: Record<string, any>;
}

export interface FindPolygonOptions {
  constraints?: Record<string, any>;
  results?: Record<string, any>;
  uniqueids?: string[];
}

export interface DeletePolygonOptions {
  constraints?: Record<string, any>;
  uniqueids?: string[];
}

// Connection types
export interface Reference {
  ref: string | number;
  class?: string;
}

export interface Connection {
  class: string;
  src: string | Reference;
  dst: string | Reference;
  created_at?: string;
  updated_at?: string;
  _uniqueid?: string;
  [key: string]: any;  // Allow any custom properties to be destructured at the base level
}

export interface EntityMetadata {
  class: string;
  created_at?: string;
  updated_at?: string;
  _uniqueid?: string;
  [key: string]: any;  
}

export interface CreateEntityInput {
  class: string;
  properties?: Record<string, any>;
}

export interface FindEntityOptions {
  with_class?: string;
  constraints?: Record<string, any>;
  results?: {
    all_properties?: boolean;
    properties?: string[];
  };
  uniqueids?: string[];
} 