import { Socket } from 'net';
import { TLSSocket } from 'tls';
import * as tls from 'tls';
import AsyncLock from 'async-lock';
import { ApertureConfig } from './types.js';
import { QueryMessage } from './proto/queryMessage.js';
import { Logger, LogLevel } from './utils/logger.js';

const PROTOCOL_VERSION = 1;

class UnauthorizedException extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'UnauthorizedException';
  }
}

class UnauthenticatedException extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'UnauthenticatedException';
  }
}

class Session {
  constructor(
    public sessionToken: string,
    public refreshToken: string,
    public sessionTokenTtl: number,
    public refreshTokenTtl: number,
    public sessionStarted: number = Date.now()
  ) {}

  valid(): boolean {
    const sessionAge = (Date.now() - this.sessionStarted) / 1000;
    const expiryOffset = parseInt(process.env.SESSION_EXPIRY_OFFSET_SEC || '10', 10);
    return sessionAge <= (this.sessionTokenTtl - expiryOffset);
  }
}

interface SharedData {
  session: Session | null;
  lock: any; // Using any since Node.js doesn't have a direct mutex equivalent
}

export class BaseClient {
  protected config: ApertureConfig;
  protected socket: Socket | TLSSocket | null = null;
  protected connected: boolean = false;
  protected authenticated: boolean = false;
  protected lastResponse: any = null;
  protected lastQueryTime: number = 0;
  protected lastQueryTimestamp: number | null = null;
  protected sharedData: SharedData;
  protected shouldAuthenticate: boolean;
  protected everConnected: boolean = false;
  protected queryConnectionErrorSuppressionDelta: number = 30000; // 30 seconds in ms

  // Helper function to truncate buffer content
  protected truncateBuffer(buf: Buffer, maxLen: number = 10): string {
    const preview = buf.toString('hex', 0, Math.min(maxLen, buf.length));
    return buf.length > maxLen ? `${preview}... (${buf.length} bytes)` : preview;
  }

  constructor(config?: Partial<ApertureConfig>) {
    // Validate required config values
    const host = config?.host ?? config?.apiUrl ?? process.env.APERTURE_HOST;
    const username = config?.username ?? process.env.APERTURE_USER;
    const password = config?.password ?? process.env.APERTURE_PASSWORD;

    if (!host) throw new Error('Host is required. Provide it in config or set APERTURE_HOST environment variable.');
    if (!username) throw new Error('Username is required. Provide it in config or set APERTURE_USER environment variable.');
    if (!password) throw new Error('Password is required. Provide it in config or set APERTURE_PASSWORD environment variable.');

    // Create full config with defaults
    this.config = {
      host,
      port: config?.port ?? 55555,
      username,
      password,
      useSsl: config?.useSsl ?? true,
      useKeepalive: config?.useKeepalive ?? true,
      retryIntervalSeconds: config?.retryIntervalSeconds ?? 1,
      retryMaxAttempts: config?.retryMaxAttempts ?? 3,
    };

    Logger.info('Initialized with config:', { 
      ...this.config,
      password: '***' // Don't log password
    });

    this.sharedData = {
      session: null,
      lock: new AsyncLock()
    };

    // Don't set shouldAuthenticate yet - wait for connect
    this.shouldAuthenticate = false;
  }

  /**
   * Get the current log level
   */
  getLogLevel(): LogLevel {
    return Logger.level;
  }

  /**
   * Set the log level for the client
   * @param level The log level to set
   */
  setLogLevel(level: LogLevel): void {
    Logger.level = level;
  }

  public async connect(reason?: string): Promise<void> {
    if (reason) {
      Logger.debug('Connecting:', reason);
    }

    // Reset connection state
    this.connected = false;
    this.authenticated = false;
    this.shouldAuthenticate = false;

    try {
      await this._connect();
      this.shouldAuthenticate = true;
      Logger.info('Connection established successfully');
    } catch (error) {
      if (error instanceof Error) {
        Logger.error(`Error connecting to server: ${this.config.host}:${this.config.port}\n${reason ? reason : ''}. error=${error.message}`);
      }
      throw error;
    }
  }

  protected async _connect(): Promise<void> {
    const CONNECT_TIMEOUT = 15000; // 15 seconds for cloud connections

    if (!this.config.host) {
      throw new Error('Host is required for connection');
    }

    try {
      // First establish TCP connection
      this.socket = new Socket();
      this.socket.setNoDelay(true);
      
      Logger.debug('Connecting to:', this.config.host, 'port:', this.config.port);

      if (this.config.useKeepalive) {
        this.socket.setKeepAlive(true, 1000);
      }

      // Connect TCP socket with more detailed error handling
      await new Promise<void>((resolve, reject) => {
        if (!this.socket) return reject(new Error('Socket not initialized'));
        
        const port = this.config.port ?? 55555;
        const host = this.config.host;
        
        // Set connection timeout
        const connectTimeout = setTimeout(() => {
          this.socket?.destroy();
          reject(new Error(`TCP Connection timeout after ${CONNECT_TIMEOUT}ms`));
        }, CONNECT_TIMEOUT);

        this.socket.once('error', (err) => {
          clearTimeout(connectTimeout);
          // Handle OS errors specifically
          if (err.code === 'ECONNREFUSED') {
            Logger.debug('Connection refused by host');
          } else if (err.code === 'ENOTFOUND') {
            Logger.debug('Host not found');
          } else if (err.code === 'ETIMEDOUT') {
            Logger.debug('Connection timed out');
          }
          Logger.debug('TCP connection error:', err.message, err.code);
          reject(err);
        });

        this.socket.connect(port, host, () => {
          clearTimeout(connectTimeout);
          Logger.debug('TCP connection established');
          resolve();
        });
      });

      // Send protocol handshake - EXACTLY like Python
      const protocol = this.config.useSsl ? 2 : 1;
      const handshake = Buffer.alloc(8);
      handshake.writeUInt32LE(PROTOCOL_VERSION, 0);
      handshake.writeUInt32LE(protocol, 4);
      
      Logger.trace('Sending protocol handshake:', { version: PROTOCOL_VERSION, protocol });
      
      await this._sendMsg(handshake);

      // Receive server response
      const response = await this._recvMsg();
      if (!response) throw new Error('No handshake response from server');

      const version = response.readUInt32LE(0);
      const serverProtocol = response.readUInt32LE(4);

      Logger.trace('Received server handshake:', { version, serverProtocol });

      if (version !== PROTOCOL_VERSION) {
        Logger.warn(`Protocol version mismatch - client: ${PROTOCOL_VERSION}, server: ${version}`);
      }

      if (serverProtocol !== protocol) {
        this.socket.destroy();
        this.connected = false;
        throw new Error('Server did not accept protocol. Aborting Connection.');
      }

      // If SSL is enabled, upgrade the connection - EXACTLY like Python
      if (this.config.useSsl) {
        try {
          Logger.debug('Starting TLS upgrade...');
          
          const tlsSocket = await new Promise<TLSSocket>((resolve, reject) => {
            const socket = tls.connect({
              socket: this.socket as Socket,
              minVersion: 'TLSv1.2',
              maxVersion: 'TLSv1.3',
              rejectUnauthorized: false,
              checkServerIdentity: () => undefined
            });

            const handshakeTimeout = setTimeout(() => {
              socket.destroy();
              reject(new Error(`TLS handshake timeout after ${CONNECT_TIMEOUT}ms`));
            }, CONNECT_TIMEOUT);

            socket.once('secureConnect', () => {
              clearTimeout(handshakeTimeout);
              Logger.debug('TLS connection established');
              Logger.debug('TLS Protocol:', socket.getProtocol());
              Logger.debug('TLS Cipher:', socket.getCipher());
              resolve(socket);
            });

            socket.once('error', (err) => {
              clearTimeout(handshakeTimeout);
              // Handle SSL errors specifically
              if (err.code === 'ECONNRESET') {
                Logger.debug('SSL connection was reset by peer');
              } else if (err.code === 'EPIPE') {
                Logger.debug('SSL connection write failed - broken pipe');
              } else if (err.message.includes('SSL')) {
                Logger.debug('SSL handshake error:', err.message);
              }
              reject(err);
            });
          });

          // Replace the socket with the TLS socket
          this.socket = tlsSocket;
          Logger.debug('TLS handshake completed successfully');
          
          // Verify socket is connected and ready
          if (!this.socket.connecting && (this.socket as TLSSocket).encrypted) {
            Logger.debug('TLS socket ready for communication');
            this.connected = true;
          } else {
            throw new Error('TLS socket not ready after upgrade');
          }
        } catch (err) {
          Logger.debug('TLS upgrade failed:', err);
          this.socket?.destroy();
          this.socket = null;
          this.connected = false;
          throw err;
        }
      }

      this.connected = true;
      Logger.debug(`Successfully connected to ${this.config.host}:${this.config.port} using ${this.config.useSsl ? 'SSL' : 'plain TCP'}`);
    } catch (error) {
      if (this.socket) {
        this.socket.destroy();
        this.socket = null;
      }
      this.connected = false;
      
      if (error instanceof Error) {
        // Check if this is a connection error that should be suppressed
        const now = Date.now();
        if (this.lastQueryTimestamp && 
            (now - this.lastQueryTimestamp) < this.queryConnectionErrorSuppressionDelta) {
          Logger.debug('Suppressing connection error due to recent query');
        } else {
          Logger.error(`Error connecting to server: ${this.config.host}:${this.config.port} - ${error.message}`);
        }
      }
      throw error;
    }
  }

  protected async _sendMsg(data: Buffer): Promise<boolean> {
    if (!this.socket) throw new Error('Socket not connected');

    // Check message size (like Python)
    if (data.length > (256 * Math.pow(2, 20))) {
      Logger.warn('Message sent is larger than default for ApertureDB Server. Server may disconnect.');
    }

    // Always send length prefix (like Python)
    const lengthBuffer = Buffer.alloc(4);
    lengthBuffer.writeUInt32LE(data.length, 0);
    const fullMessage = Buffer.concat([lengthBuffer, data]);

    Logger.trace('_sendMsg:', {
      totalLength: fullMessage.length
    });

    return new Promise((resolve, reject) => {
      const socket = this.socket!;

      // More comprehensive socket state check
      if (!socket.writable || socket.destroyed) {
        this.connected = false;
        reject(new Error('Socket is not in a valid state for writing'));
        return;
      }

      const onError = (err: Error) => {
        cleanup();
        this.connected = false;
        
        // Special handling for EPIPE and other connection errors
        if (err.message.includes('EPIPE') || err.message.includes('ECONNRESET')) {
          reject(new Error('Connection lost during write'));
        } else {
          reject(err);
        }
      };

      const onClose = (hadError: boolean) => {
        cleanup();
        this.connected = false;
        reject(new Error('Socket closed while sending'));
      };

      const cleanup = () => {
        socket.removeListener('error', onError);
        socket.removeListener('close', onClose);
      };

      socket.on('error', onError);
      socket.on('close', onClose);

      try {
        // Use socket.write with callback to get write success
        socket.write(fullMessage, (err) => {
          cleanup();
          if (err) {
            this.connected = false;
            if (err.message.includes('EPIPE')) {
              reject(new Error('Connection lost during write'));
            } else {
              reject(err);
            }
          } else {
            // If we get here, the write succeeded
            resolve(true);
          }
        });
      } catch (err) {
        cleanup();
        this.connected = false;
        reject(err);
      }
    });
  }

  protected async _recvMsg(): Promise<Buffer | null> {
    if (!this.socket) throw new Error('Socket not connected');
    const socket = this.socket;

    return new Promise<Buffer | null>((resolve, reject) => {
      let lengthBuffer = Buffer.alloc(0);
      let messageBuffer = Buffer.alloc(0);
      let messageLength = -1;
      let bytesRead = 0;

      // Check socket state first
      if (!socket.readable || socket.destroyed) {
        this.connected = false;
        reject(new Error('Socket is not in a valid state for reading'));
        return;
      }

      const onData = (chunk: Buffer) => {
        try {
          Logger.trace('_recvMsg: Received chunk:', {
            chunkLength: chunk.length
          });

          // Still reading length prefix
          if (messageLength === -1) {
            lengthBuffer = Buffer.concat([lengthBuffer, chunk]);
            Logger.trace('_recvMsg: Length buffer:', {
              length: lengthBuffer.length
            });

            if (lengthBuffer.length >= 4) {
              messageLength = lengthBuffer.readUInt32LE(0);
              Logger.trace('_recvMsg: Message length from prefix:', messageLength);
              
              // Handle empty message
              if (messageLength === 0) {
                Logger.trace('_recvMsg: Empty message received');
                cleanup();
                return resolve(Buffer.alloc(0));
              }

              // Allocate buffer for full message
              messageBuffer = Buffer.alloc(messageLength);

              // If we have extra data beyond length prefix, copy it to message buffer
              if (lengthBuffer.length > 4) {
                const extraData = lengthBuffer.slice(4);
                const bytesToCopy = Math.min(extraData.length, messageLength);
                extraData.copy(messageBuffer, 0, 0, bytesToCopy);
                bytesRead = bytesToCopy;

                Logger.trace('_recvMsg: Extra data after length prefix:', {
                  extraLength: extraData.length,
                  copied: bytesToCopy
                });

                // If we have the complete message already
                if (bytesRead === messageLength) {
                  Logger.trace('_recvMsg: Complete message received:', {
                    length: messageBuffer.length
                  });
                  cleanup();
                  resolve(messageBuffer);
                }
              }
            }
          }
          // Reading message body
          else {
            const remainingBytes = messageLength - bytesRead;
            const bytesToCopy = Math.min(chunk.length, remainingBytes);
            chunk.copy(messageBuffer, bytesRead, 0, bytesToCopy);
            bytesRead += bytesToCopy;

            Logger.trace('_recvMsg: Reading message body:', {
              totalLength: messageLength,
              bytesRead,
              newChunkLength: bytesToCopy,
              remaining: messageLength - bytesRead
            });

            // Check if we have the complete message
            if (bytesRead === messageLength) {
              Logger.trace('_recvMsg: Complete message received:', {
                length: messageBuffer.length
              });
              cleanup();
              resolve(messageBuffer);
            }
            // Check for overflow (shouldn't happen, but just in case)
            else if (bytesRead > messageLength) {
              cleanup();
              reject(new Error('Received more data than expected'));
            }
          }
        } catch (err) {
          cleanup();
          reject(err);
        }
      };

      const onError = (err: Error) => {
        cleanup();
        this.connected = false;
        reject(err);
      };

      const onEnd = () => {
        cleanup();
        this.connected = false;
        if (messageLength === -1) {
          // Connection ended while reading length
          if (lengthBuffer.length === 0) {
            resolve(null);
          } else {
            reject(new Error('Connection ended while reading message length'));
          }
        } else if (bytesRead < messageLength) {
          // Connection ended while reading body
          reject(new Error('Connection ended while reading message body'));
        } else {
          // Normal end after complete message
          resolve(messageBuffer);
        }
      };

      const onClose = (hadError: boolean) => {
        cleanup();
        this.connected = false;
        if (hadError) {
          reject(new Error('Socket closed with error'));
        } else {
          onEnd();
        }
      };

      const cleanup = () => {
        socket.removeListener('data', onData);
        socket.removeListener('error', onError);
        socket.removeListener('end', onEnd);
        socket.removeListener('close', onClose);
      };

      socket.on('data', onData);
      socket.on('error', onError);
      socket.on('end', onEnd);
      socket.on('close', onClose);
    });
  }

  protected async _authenticate(user: string, password: string = '', token: string = ''): Promise<void> {
    Logger.debug('_authenticate: Starting authentication process', { user, hasPassword: !!password, hasToken: !!token });
    
    // Match Python's query structure exactly
    const query = [{
      "Authenticate": {
        "username": user
      } as Record<string, any>
    }];

    if (password) {
      query[0]["Authenticate"]["password"] = password;
      Logger.debug('_authenticate: Using password authentication');
    } else if (token) {
      query[0]["Authenticate"]["token"] = token;
      Logger.debug('_authenticate: Using token authentication');
    } else {
      Logger.debug('_authenticate: No password or token provided');
      throw new Error('Either password or token must be specified for authentication');
    }

    Logger.trace('_authenticate: Sending authentication query:', JSON.stringify(query));
    try {
      // Use _query directly like Python does
      const [response] = await this._query(query, []);
      Logger.trace('_authenticate: Received authentication response:', JSON.stringify(response));

      if (!Array.isArray(response) || !("Authenticate" in response[0])) {
        Logger.error('_authenticate: Invalid response format', { response });
        throw new Error("Unexpected response from server upon authenticate request: " + JSON.stringify(response));
      }

      const sessionInfo = response[0]["Authenticate"];
      Logger.debug('_authenticate: Session info:', { status: sessionInfo["status"], hasToken: !!sessionInfo["session_token"] });
      
      if (sessionInfo["status"] !== 0) {
        Logger.error('_authenticate: Authentication failed', { status: sessionInfo["status"], info: sessionInfo["info"] });
        throw new Error(sessionInfo["info"]);
      }

      Logger.debug('_authenticate: Creating new session');
      this.sharedData.session = new Session(
        sessionInfo["session_token"],
        sessionInfo["refresh_token"],
        sessionInfo["session_token_expires_in"],
        sessionInfo["refresh_token_expires_in"],
        Date.now()
      );
      Logger.info('Authentication completed successfully');
    } catch (error) {
      Logger.error('_authenticate: Error during authentication:', error);
      throw error;
    }
  }

  protected async _checkSessionStatus(): Promise<void> {
    if (!this.sharedData.session) return;

    if (!this.sharedData.session.valid()) {
      await this.sharedData.lock.acquire('session', async () => {
        await this._refreshToken();
      });
    }
  }

  protected async _refreshToken(): Promise<void> {
    if (!this.sharedData.session) return;

    const query = [{
      "RefreshToken": {
        "refresh_token": this.sharedData.session.refreshToken
      }
    }];

    const [response] = await this._query(query, [], false);

    Logger.info('Refresh token response:', response);

    if (Array.isArray(response)) {
      const sessionInfo = response[0].RefreshToken;
      if (sessionInfo.status !== 0) {
        // Refresh token failed, need to re-authenticate
        this.authenticated = false;
        this.shouldAuthenticate = true;
        this.sharedData.session = null;
        await this.authenticate(
          this.sharedData,
          this.config.username,
          this.config.password
        );
        throw new UnauthorizedException(JSON.stringify(response));
      }

      this.sharedData.session = new Session(
        sessionInfo.session_token,
        sessionInfo.refresh_token,
        sessionInfo.session_token_expires_in,
        sessionInfo.refresh_token_expires_in,
        Date.now()
      );
    } else {
      throw new UnauthorizedException(JSON.stringify(response));
    }
  }

  protected async _query(query: any[], blobArray: Buffer[] = [], tryResume: boolean = true): Promise<[any, Buffer[]]> {
    let responseBlobArray: Buffer[] = [];
    
    // Convert query to protobuf message
    const queryStr = typeof query === 'string' ? query : JSON.stringify(query);
    Logger.trace('_query: Sending query:', queryStr);
    
    const queryMessage = new QueryMessage(queryStr, blobArray);

    // Add session token if we have one
    if (this.sharedData.session?.sessionToken) {
      queryMessage.setToken(this.sharedData.session.sessionToken);
      Logger.trace('_query: Added session token to query');
    }
    
    const queryBuffer = queryMessage.toBuffer();

    try {
      // Ensure we're connected before sending
      if (!this.connected || !this.socket?.writable) {
        Logger.debug('_query: Socket not connected or not writable, reconnecting...');
        if (this.socket) {
          this.socket.destroy();
          this.socket = null;
        }
        this.connected = false;
        await this.connect("Reconnecting before query attempt");
        
        // After reconnect, we need to re-authenticate
        if (!this.authenticated || this.shouldAuthenticate) {
          await this.ensureAuthenticated();
        }
      }

      Logger.trace('_query: Sending message...');
      const sendResult = await this._sendMsg(queryBuffer);
      Logger.trace('_query: Send result:', sendResult);

      if (sendResult) {
        Logger.trace('_query: Waiting for response...');
        const response = await this._recvMsg();
        Logger.trace('_query: Received response:', response?.length ?? 0, 'bytes');
        
        if (response) {
          // Parse response using protobuf
          Logger.trace('_query: Parsing protobuf response...');
          const responseMessage = QueryMessage.fromBuffer(response);
          const responseStr = responseMessage.getJson();
          Logger.trace('_query: Parsed response:', {
            message: responseStr,
            blobs: responseMessage.getBlobs().map(blob => `${blob.length} bytes`),
            blobCount: responseMessage.getBlobs().length,
            totalBlobBytes: responseMessage.getBlobs().reduce((acc, blob) => acc + blob.length, 0)
          });
          
          if (responseStr) {
            try {
              this.lastResponse = JSON.parse(responseStr);
              responseBlobArray = responseMessage.getBlobs();
              if (this.lastResponse) {
                Logger.debug('Query completed successfully');
                return [this.lastResponse, responseBlobArray];
              }
            } catch (e) {
              Logger.error('_query: Failed to parse response JSON:', e);
            }
          } else {
            Logger.error('_query: Response message had no JSON content');
          }
        } else {
          Logger.error('_query: Received null response from _recvMsg');
        }
      } else {
        Logger.error('_query: Failed to send message');
      }

      throw new Error('Failed to get valid response from server');
    } catch (error) {
      // Handle connection cleanup
      if (this.socket) {
        this.socket.destroy();
        this.socket = null;
      }
      this.connected = false;

      if (error instanceof Error) {
        Logger.error('_query: Error details:', {
          name: error.name,
          message: error.message,
          stack: error.stack
        });
      }

      throw error;
    }
  }

  protected async _renewSession(): Promise<void> {
    let count = 0;
    while (count < 3) {
      try {
        await this._checkSessionStatus();
        break;
      } catch (error) {
        if (error instanceof UnauthorizedException) {
          Logger.warn(`[Attempt ${count + 1} of 3] Failed to refresh token.`, error);
          await new Promise(resolve => setTimeout(resolve, 1000));
          count++;
        } else {
          throw error;
        }
      }
    }
  }

  public async ensureAuthenticated(): Promise<void> {
    return this.sharedData.lock.acquire('session', async () => {
      Logger.debug('ensureAuthenticated: Starting authentication check');
      Logger.debug('ensureAuthenticated: Connection status:', { connected: this.connected, authenticated: this.authenticated, shouldAuthenticate: this.shouldAuthenticate });
      
      // First ensure we're connected
      if (!this.connected) {
        Logger.debug('ensureAuthenticated: Not connected, connecting...');
        await this.connect();
        Logger.debug('ensureAuthenticated: Connection established');
      }

      // Then handle authentication if needed
      if (!this.authenticated || this.shouldAuthenticate) {
        Logger.debug('ensureAuthenticated: Need to authenticate', { authenticated: this.authenticated, shouldAuthenticate: this.shouldAuthenticate });
        // No need to acquire lock in authenticate since we already have it
        await this._authenticate(this.config.username, this.config.password);
        this.authenticated = true;
        this.shouldAuthenticate = false;
        Logger.debug('ensureAuthenticated: Authentication completed');
        return;
      }

      // Only check session if we're already authenticated and it's invalid
      if (this.sharedData.session && !this.sharedData.session.valid()) {
        Logger.debug('ensureAuthenticated: Session expired, refreshing');
        await this._refreshToken();
      }
    });
  }

  async authenticate(sharedData: SharedData, user: string, password?: string, token?: string): Promise<void> {
    Logger.debug('authenticate: Starting with params:', { user, hasPassword: !!password, hasToken: !!token });
    // Use the same lock as ensureAuthenticated
    await this.sharedData.lock.acquire('session', async () => {
      Logger.debug('authenticate: Acquired lock, checking state:', { authenticated: this.authenticated, hasSession: !!sharedData.session });
      if (!this.authenticated) {
        if (sharedData.session === null) {
          Logger.debug('authenticate: No session, calling _authenticate');
          await this._authenticate(user, password || this.config.password, token || '');
        } else {
          Logger.debug('authenticate: Using existing session');
          this.sharedData = sharedData;
        }
        this.authenticated = true;
        Logger.debug('authenticate: Marked as authenticated');
      } else {
        Logger.debug('authenticate: Already authenticated, skipping');
      }
    });
  }

  getLastResponseStr(): string {
    return JSON.stringify(this.lastResponse, null, 4);
  }

  printLastResponse(): void {
    Logger.info(this.getLastResponseStr());
  }

  getLastQueryTime(): number {
    return this.lastQueryTime;
  }

  getResponse(): any {
    return this.lastResponse;
  }

  getBlobs(): Buffer[] {
    return [];
  }

  lastQueryOk(): boolean {
    return this.checkStatus(this.lastResponse) >= 0;
  }

  checkStatus(jsonRes: any): number {
    let status = -2;
    if (typeof jsonRes === 'object' && jsonRes !== null) {
      if (!('status' in jsonRes)) {
        const firstKey = Object.keys(jsonRes)[0];
        status = this.checkStatus(jsonRes[firstKey]);
      } else {
        status = jsonRes.status;
      }
    } else if (Array.isArray(jsonRes)) {
      if (!('status' in jsonRes[0])) {
        status = this.checkStatus(jsonRes[0]);
      } else {
        status = jsonRes[0].status;
      }
    }
    return status;
  }

  public async query<T = any>(q: any, blobs: Buffer[] = []): Promise<[T, Buffer[]]> {
    await this.ensureAuthenticated();

    try {
      const start = Date.now();
      let [response, responseBlobs] = await this._query(q, blobs);

      if (!Array.isArray(response) && response.info === 'Not Authenticated!') {
        Logger.warn(`Session expired while query was sent. Retrying... ${JSON.stringify(this.config)}`);
        await this._renewSession();
        [response, responseBlobs] = await this._query(q, blobs);
      }

      this.lastQueryTime = (Date.now() - start) / 1000;
      this.lastQueryTimestamp = Date.now();
      return [response, responseBlobs];
    } catch (error) {
      Logger.error('Failed to query', error);
      throw error;
    }
  }
} 