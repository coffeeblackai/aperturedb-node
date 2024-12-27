import protobuf from 'protobufjs';
import { fileURLToPath } from 'url';
import { join, dirname } from 'path';

interface QueryMessageData {
  json: string;
  blobs: Buffer[];
  token: string;
}

type ProtobufQueryMessage = protobuf.Message<QueryMessageData> & QueryMessageData;

// Load the proto file
const root = new protobuf.Root();
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const protoPath = join(__dirname, 'queryMessage.proto');
const QueryMessageType = root.loadSync(protoPath).lookupType('VDMS.protobufs.queryMessage');

export class QueryMessage {
  private message: ProtobufQueryMessage;

  constructor(json: string = '', blobs: Buffer[] = [], token: string = '') {
    this.message = QueryMessageType.create({
      json,
      blobs,
      token
    }) as ProtobufQueryMessage;
  }

  public setJson(json: string): void {
    (this.message as any).json = json;
  }

  public setToken(token: string): void {
    (this.message as any).token = token;
  }

  public addBlob(blob: Buffer): void {
    if (!this.message.blobs) {
      this.message.blobs = [];
    }
    this.message.blobs.push(blob);
  }

  public setBlobs(blobs: Buffer[]): void {
    (this.message as any).blobs = blobs;
  }

  public toBuffer(): Buffer {
    const errMsg = QueryMessageType.verify(this.message);
    if (errMsg) throw Error(errMsg);
    
    const encoded = QueryMessageType.encode(this.message).finish();
    return Buffer.from(encoded);
  }

  public static fromBuffer(buffer: Buffer): QueryMessage {
    const decoded = QueryMessageType.decode(buffer) as unknown as ProtobufQueryMessage;
    const message = new QueryMessage();
    message.setJson(decoded.json);
    message.setToken(decoded.token);
    if (decoded.blobs) {
      message.setBlobs(decoded.blobs.map(b => Buffer.from(b)));
    }
    return message;
  }

  public getJson(): string {
    return this.message.json;
  }

  public getToken(): string {
    return this.message.token;
  }

  public getBlobs(): Buffer[] {
    return this.message.blobs || [];
  }
} 