import { Socket } from 'net';
import { TLSSocket } from 'tls';
import * as tls from 'tls';
import AsyncLock from 'async-lock';
import { ApertureConfig } from './types.js';
import { QueryMessage } from './proto/queryMessage.js';
import { Logger, LogLevel } from './utils/logger.js';
import { QueryExecutor } from './parallel.js';

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

// Add connection pool management
interface PooledSocket {
  socket: Socket | TLSSocket;
  lastUsed: number;
  busy: boolean;
}

class ConnectionPool {
  private static instance: ConnectionPool;
  private pool: Map<string, PooledSocket[]> = new Map();
  private maxPoolSize: number = 5;
  private maxIdleTime: number = 60000; // 60 seconds
  private cleanupInterval: NodeJS.Timeout;
  private sharedData: SharedData = {
    session: null,
    lock: new AsyncLock()
  };

  private constructor() {
    // Create cleanup interval and unref it so it doesn't keep the process alive
    this.cleanupInterval = setInterval(() => this.cleanup(), 30000).unref();
  }

  static getInstance(): ConnectionPool {
    if (!ConnectionPool.instance) {
      ConnectionPool.instance = new ConnectionPool();
    }
    return ConnectionPool.instance;
  }

  getConnection(key: string): PooledSocket | undefined {
    const connections = this.pool.get(key) || [];
    // Find first non-busy connection that is still valid
    return connections.find(conn => !conn.busy && conn.socket.writable && !conn.socket.destroyed);
  }

  addConnection(key: string, socket: Socket | TLSSocket): void {
    const connections = this.pool.get(key) || [];
    if (connections.length < this.maxPoolSize) {
      connections.push({
        socket,
        lastUsed: Date.now(),
        busy: false
      });
      this.pool.set(key, connections);
    } else {
      // If pool is full, destroy the socket
      socket.destroy();
    }
  }

  markBusy(key: string, socket: Socket | TLSSocket, busy: boolean): void {
    const connections = this.pool.get(key) || [];
    const connection = connections.find(conn => conn.socket === socket);
    if (connection) {
      connection.busy = busy;
      connection.lastUsed = Date.now();
    }
  }

  removeConnection(key: string, socket: Socket | TLSSocket): void {
    const connections = this.pool.get(key) || [];
    const index = connections.findIndex(conn => conn.socket === socket);
    if (index !== -1) {
      // Ensure socket is properly destroyed
      try {
        connections[index].socket.end();
        connections[index].socket.destroy();
      } catch (err) {
        // Ignore errors during cleanup
      }
      connections.splice(index, 1);
      if (connections.length === 0) {
        this.pool.delete(key);
      } else {
        this.pool.set(key, connections);
      }
    }
  }

  private cleanup(): void {
    const now = Date.now();
    for (const [key, connections] of this.pool.entries()) {
      const activeConnections = connections.filter(conn => {
        const idle = now - conn.lastUsed > this.maxIdleTime;
        const invalid = !conn.socket.writable || conn.socket.destroyed;
        if ((idle && !conn.busy) || invalid) {
          try {
            conn.socket.end();
            conn.socket.destroy();
          } catch (err) {
            // Ignore errors during cleanup
          }
          return false;
        }
        return true;
      });
      if (activeConnections.length === 0) {
        this.pool.delete(key);
      } else {
        this.pool.set(key, activeConnections);
      }
    }
  }

  getSharedData(): SharedData {
    return this.sharedData;
  }

  destroy(): void {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
      this.cleanupInterval = null as any;
    }
    for (const connections of this.pool.values()) {
      connections.forEach(conn => {
        try {
          conn.socket.end();
          conn.socket.destroy();
        } catch (err) {
          // Ignore errors during cleanup
        }
      });
    }
    this.pool.clear();
    this.sharedData.session = null;
  }

  static destroyInstance(): void {
    if (ConnectionPool.instance) {
      ConnectionPool.instance.destroy();
      ConnectionPool.instance = null as any;
    }
  }
}

export class BaseClient implements QueryExecutor {
  protected config: ApertureConfig;
  protected socket: Socket | TLSSocket | null = null;
  protected connected: boolean = false;
  protected authenticated: boolean = false;
  protected lastResponse: any = null;
  protected lastQueryTime: number = 0;
  protected lastQueryTimestamp: number | null = null;
  protected shouldAuthenticate: boolean;
  protected everConnected: boolean = false;
  protected queryConnectionErrorSuppressionDelta: number = 30000;
  protected connectionPool: ConnectionPool;
  protected messageCache: Map<string, Buffer> = new Map();
  protected queryComplete: boolean = false;

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

    // Don't set shouldAuthenticate yet - wait for connect
    this.shouldAuthenticate = false;
    this.connectionPool = ConnectionPool.getInstance();
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
    const CONNECT_TIMEOUT = 15000;
    const poolKey = `${this.config.host}:${this.config.port}`;
    
    try {
      // Check connection pool first
      const pooledSocket = this.connectionPool.getConnection(poolKey);
      if (pooledSocket) {
        // Validate the pooled connection before reuse
        if (await this.validateConnection(pooledSocket.socket)) {
          this.socket = pooledSocket.socket;
          this.connectionPool.markBusy(poolKey, this.socket, true);
          this.connected = true;
          Logger.debug('Reusing validated pooled connection');
          return;
        } else {
          Logger.debug('Pooled connection validation failed, removing from pool');
          this.connectionPool.removeConnection(poolKey, pooledSocket.socket);
        }
      }

      // Create new connection if none available in pool
      this.socket = new Socket();
      this.socket.setNoDelay(true);
      
      if (this.config.useKeepalive) {
        this.socket.setKeepAlive(true, 1000);
      }

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
      if (!response) {
        // For handshake, we do need a response to verify protocol version
        // Clean up on handshake failure
        Logger.debug('_connect: No handshake response, will retry connection');
        if (this.socket) {
          this.connectionPool.removeConnection(poolKey, this.socket);
          this.socket.destroy();
          this.socket = null;
        }
        this.connected = false;
        throw new Error('No handshake response from server');
      }

      try {
        const version = response.readUInt32LE(0);
        const serverProtocol = response.readUInt32LE(4);

        Logger.trace('Received server handshake:', { version, serverProtocol });

        if (version !== PROTOCOL_VERSION) {
          Logger.warn(`Protocol version mismatch - client: ${PROTOCOL_VERSION}, server: ${version}`);
        }

        if (serverProtocol !== protocol) {
          if (this.socket) {
            this.connectionPool.removeConnection(poolKey, this.socket);
            this.socket.destroy();
            this.socket = null;
          }
          this.connected = false;
          throw new Error('Server did not accept protocol. Aborting Connection.');
        }
      } catch (error) {
        // Handle buffer reading errors
        Logger.debug('_connect: Error reading handshake response:', error);
        if (this.socket) {
          this.connectionPool.removeConnection(poolKey, this.socket);
          this.socket.destroy();
          this.socket = null;
        }
        this.connected = false;
        throw new Error('Invalid handshake response from server');
      }

      // If SSL is enabled, upgrade the connection
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

      // Add new connection to pool
      this.connectionPool.addConnection(poolKey, this.socket);
      this.connectionPool.markBusy(poolKey, this.socket, true);
    } catch (error) {
      if (this.socket) {
        this.connectionPool.removeConnection(poolKey, this.socket);
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
      // Use _query directly like Python does, but with tryResume=false to prevent loops
      const [response] = await this._query(query, [], false);
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
      this.authenticated = true;
      this.shouldAuthenticate = false;
      Logger.info('Authentication completed successfully');
    } catch (error) {
      Logger.error('_authenticate: Error during authentication:', error);
      this.authenticated = false;
      this.shouldAuthenticate = true;
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
    const poolKey = `${this.config.host}:${this.config.port}`;
    this.queryComplete = false;
    let retryCount = 0;
    const maxRetries = 3;
    const baseDelay = 1000;

    while (retryCount <= maxRetries) {
      try {
        // Ensure connection
        if (!this.connected || !this.socket?.writable) {
          await this.connect("Reconnecting before query attempt");
          if (!this.authenticated || this.shouldAuthenticate) {
            await this.ensureAuthenticated();
          }
        }

        // Cache message if possible
        const queryStr = typeof query === 'string' ? query : JSON.stringify(query);
        const cacheKey = `${queryStr}-${this.sharedData.session?.sessionToken || ''}`;
        let queryBuffer = this.messageCache.get(cacheKey);

        if (!queryBuffer) {
          const queryMessage = new QueryMessage(queryStr, blobArray);
          if (this.sharedData.session?.sessionToken) {
            queryMessage.setToken(this.sharedData.session.sessionToken);
          }
          queryBuffer = queryMessage.toBuffer();
          this.messageCache.set(cacheKey, queryBuffer);
        }

        const sendResult = await this._sendMsg(queryBuffer);
        
        if (sendResult) {
          const response = await this._recvMsg();
          if (response) {
            const responseMessage = QueryMessage.fromBuffer(response);
            const responseStr = responseMessage.getJson();
            
            try {
              if (responseStr) {
                this.lastResponse = JSON.parse(responseStr);
                const responseBlobArray = responseMessage.getBlobs();
                this.queryComplete = true;
                return [this.lastResponse, responseBlobArray];
              } else {
                // Empty response is valid
                this.queryComplete = true;
                return [null, []];
              }
            } catch (e) {
              Logger.error('_query: Failed to parse response JSON:', e);
              throw e;
            }
          } else {
            if (retryCount < maxRetries) {
              Logger.debug(`No response buffer on attempt ${retryCount + 1}/${maxRetries + 1}, retrying...`);
              const delay = baseDelay * Math.pow(2, retryCount) * (0.5 + Math.random());
              await new Promise(resolve => setTimeout(resolve, delay));
              retryCount++;
              continue;
            }
            // No response buffer is also valid
            this.queryComplete = true;
            return [null, []];
          }
        }

        Logger.debug('Send failed, returning empty response');
        return [null, []];

      } catch (error) {
        // Only destroy socket for actual connection errors
        if (error instanceof Error && 
            (error.message.includes('ECONNRESET') || 
             error.message.includes('EPIPE') || 
             error.message.includes('socket') ||
             error.message.includes('connection') ||
             error.message.includes('network'))) {
          if (this.socket) {
            this.connectionPool.removeConnection(poolKey, this.socket);
            this.socket.destroy();
            this.socket = null;
          }
          this.connected = false;
        }
        
        if (retryCount < maxRetries) {
          Logger.debug(`Query error on attempt ${retryCount + 1}/${maxRetries + 1}, retrying...`, error);
          const delay = baseDelay * Math.pow(2, retryCount) * (0.5 + Math.random());
          await new Promise(resolve => setTimeout(resolve, delay));
          retryCount++;
          continue;
        }
        throw error;
      } finally {
        if (this.socket) {
          const poolKey = `${this.config.host}:${this.config.port}`;
          if (this.queryComplete) {
            // Only keep the connection in the pool if the query completed successfully
            this.connectionPool.markBusy(poolKey, this.socket, false);
          } else {
            // If query didn't complete, remove the connection from pool
            this.connectionPool.removeConnection(poolKey, this.socket);
            this.socket.destroy();
            this.socket = null;
            this.connected = false;
          }
        }
      }
    }

    return [null, []];
  }

  private async checkConnectionHealth(): Promise<boolean> {
    try {
      const emptyBufferArray: Buffer[] = [];
      const [response] = await this._query([{ "Ping": {} }], emptyBufferArray, false);
      return response?.Ping?.status === 0;
    } catch {
      return false;
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
        try {
          // No need to acquire lock in authenticate since we already have it
          await this._authenticate(this.config.username, this.config.password);
          Logger.debug('ensureAuthenticated: Authentication completed');
          return;
        } catch (error) {
          Logger.error('ensureAuthenticated: Authentication failed:', error);
          // Reset connection state on authentication failure
          this.connected = false;
          this.authenticated = false;
          this.shouldAuthenticate = true;
          throw error;
        }
      }

      // Only check session if we're already authenticated and it's invalid
      if (this.sharedData.session && !this.sharedData.session.valid()) {
        Logger.debug('ensureAuthenticated: Session expired, refreshing');
        try {
          await this._refreshToken();
        } catch (error) {
          Logger.error('ensureAuthenticated: Session refresh failed:', error);
          // Reset connection state on refresh failure
          this.connected = false;
          this.authenticated = false;
          this.shouldAuthenticate = true;
          throw error;
        }
      }
    });
  }

  async authenticate(sharedData: SharedData, user: string, password?: string, token?: string): Promise<void> {
    Logger.debug('authenticate: Starting with params:', { user, hasPassword: !!password, hasToken: !!token });
    // Use the same lock as ensureAuthenticated
    await this.sharedData.lock.acquire('session', async () => {
      Logger.debug('authenticate: Acquired lock, checking state:', { authenticated: this.authenticated, hasSession: !!this.sharedData.session });
      if (!this.authenticated) {
        if (this.sharedData.session === null) {
          Logger.debug('authenticate: No session, calling _authenticate');
          await this._authenticate(user, password || this.config.password, token || '');
        } else {
          Logger.debug('authenticate: Using existing session');
          // Instead of assigning sharedData directly, we'll copy the session
          this.connectionPool.getSharedData().session = sharedData.session;
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
      if ('status' in jsonRes) {
        status = jsonRes.status;
      } else {
        // Check first level
        const firstKey = Object.keys(jsonRes)[0];
        if (firstKey) {
          const firstValue = jsonRes[firstKey];
          if (typeof firstValue === 'object' && firstValue !== null) {
            if ('status' in firstValue) {
              status = firstValue.status;
            }
          }
        }
      }
    } else if (Array.isArray(jsonRes)) {
      // For arrays, check each element until we find a valid status
      for (const item of jsonRes) {
        const itemStatus = this.checkStatus(item);
        if (itemStatus !== -2) {
          status = itemStatus;
          break;
        }
      }
    }
    return status;
  }

  public async query<T = any>(q: any, blobs: Buffer[] = []): Promise<[T, Buffer[]]> {
    await this.ensureAuthenticated();
    const start = Date.now();
    const [response, responseBlobs] = await this._query(q, blobs);
    this.lastQueryTime = (Date.now() - start) / 1000;
    this.lastQueryTimestamp = Date.now();
    return [response, responseBlobs];
  }

  // Add cleanup method
  public async destroy(): Promise<void> {
    // Wait for any pending operations to complete
    await this.sharedData.lock.acquire('session', async () => {
      if (this.socket) {
        const poolKey = `${this.config.host}:${this.config.port}`;
        this.connectionPool.removeConnection(poolKey, this.socket);
        
        // Properly close the socket
        await new Promise<void>((resolve) => {
          if (!this.socket) {
            resolve();
            return;
          }

          const cleanup = () => {
            this.socket?.removeListener('close', onClose);
            this.socket?.removeListener('error', onError);
            resolve();
          };

          const onClose = () => cleanup();
          const onError = () => cleanup();

          this.socket.once('close', onClose);
          this.socket.once('error', onError);

          try {
            this.socket.end(() => {
              this.socket?.destroy();
              cleanup();
            });
          } catch (err) {
            cleanup();
          }
        });

        this.socket = null;
      }

      this.connected = false;
      this.authenticated = false;
      this.messageCache.clear();
      this.sharedData.session = null;
    });
  }

  protected get sharedData(): SharedData {
    return this.connectionPool.getSharedData();
  }

  protected async validateConnection(socket: Socket | TLSSocket): Promise<boolean> {
    // First do basic socket state checks
    if (!socket.writable || socket.destroyed || socket.connecting || socket.pending) {
      Logger.debug('validateConnection: Socket in invalid state:', {
        writable: socket.writable,
        destroyed: socket.destroyed,
        connecting: socket.connecting,
        pending: socket.pending
      });
      return false;
    }

    // For TLS sockets, verify encryption
    if (this.config.useSsl && socket instanceof TLSSocket) {
      if (!socket.encrypted || !socket.authorized) {
        Logger.debug('validateConnection: TLS socket not properly secured');
        return false;
      }
    }
    
    this.queryComplete = false;
    const poolKey = `${this.config.host}:${this.config.port}`;
    
    try {
      // Send a ping/heartbeat query
      const pingQuery = [{ "Ping": {} }];
      const queryMessage = new QueryMessage(JSON.stringify(pingQuery), []);
      if (this.sharedData.session?.sessionToken) {
        queryMessage.setToken(this.sharedData.session.sessionToken);
      }
      
      await this._sendMsg(queryMessage.toBuffer());
      const response = await this._recvMsg();
      
      if (!response) {
        // Empty response is valid for ping
        this.queryComplete = true;
        return true;
      }
      
      const responseMessage = QueryMessage.fromBuffer(response);
      const responseStr = responseMessage.getJson();
      if (!responseStr) {
        // Empty response string is also valid
        this.queryComplete = true;
        return true;
      }
      
      const pingResponse = JSON.parse(responseStr);
      this.queryComplete = true;
      
      const isValid = Array.isArray(pingResponse) && pingResponse[0]?.Ping?.status === 0;
      if (!isValid) {
        Logger.debug('validateConnection: Invalid ping response:', pingResponse);
      }
      return isValid;
    } catch (error) {
      Logger.debug('validateConnection failed:', error);
      return false;
    } finally {
      if (!this.queryComplete) {
        this.connectionPool.removeConnection(poolKey, socket);
        socket.destroy();
      }
    }
  }
} 