import { BaseClient } from './base.js';
import { ApertureConfig } from './types.js';
import { LogLevel } from './utils/logger.js';
import { PolygonClient } from './polygon.js';
import { FrameClient } from './frame.js';
import { ClipClient } from './clip.js';

export class ApertureClient extends BaseClient {
  private _polygon: PolygonClient;
  private _frame: FrameClient;
  private _clip: ClipClient;

  constructor(config?: Partial<ApertureConfig>) {
    super(config);
    this._polygon = new PolygonClient(this);
    this._frame = new FrameClient(this);
    this._clip = new ClipClient(this);
  }

  /**
   * Get the current log level
   */
  getLogLevel(): LogLevel {
    return super.getLogLevel();
  }

  /**
   * Set the log level for the client
   * @param level The log level to set
   */
  setLogLevel(level: LogLevel): void {
    super.setLogLevel(level);
  }

  get polygon(): PolygonClient {
    return this._polygon;
  }

  get frame(): FrameClient {
    return this._frame;
  }

  get clip(): ClipClient {
    return this._clip;
  }
} 