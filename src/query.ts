import { ObjectType, QueryParams, QueryCommand, Sort, Constraints, Operations } from './types/query';
import * as struct from 'python-struct';

export class QueryBuilder {
  static findCommand(oclass: string | ObjectType, params: QueryParams): QueryCommand {
    return this.buildCommand(oclass, params, 'Find');
  }

  static addCommand(oclass: string | ObjectType, params: QueryParams): QueryCommand {
    return this.buildCommand(oclass, params, 'Add');
  }

  private static buildCommand(oclass: string | ObjectType, params: QueryParams, operation: string): QueryCommand {
    const objectClass = typeof oclass === 'string' ? oclass : oclass;
    const command: QueryCommand = {};
    
    // Handle results section consistently
    if (!params.results) {
      params.results = {
        all_properties: true
      };
    }

    // Handle references
    if (params._ref) {
      params._ref = params._ref;
    }

    // Handle src_query and dst_query for connections
    if (params.src_query) {
      params.src_query = {
        FindEntity: {
          _ref: params.src_query.FindEntity._ref,
          constraints: params.src_query.FindEntity.constraints
        }
      };
    }

    if (params.dst_query) {
      params.dst_query = {
        FindEntity: {
          _ref: params.dst_query.FindEntity._ref,
          constraints: params.dst_query.FindEntity.constraints
        }
      };
    }

    const members = Object.values(ObjectType);
    if (objectClass.startsWith('_')) {
      if (members.includes(objectClass as ObjectType)) {
        command[`${operation}${objectClass.slice(1)}`] = params;
      } else {
        throw new Error(`Invalid Object type. Should not begin with _, except for ${members.join(', ')}`);
      }
    } else {
      if (operation === 'Find') {
        params.with_class = objectClass;
      } else {
        params.class = objectClass;
      }
      command[`${operation}Entity`] = params;
    }

    return command;
  }
}

export class Query {
  private dbObject: string = 'Entity';
  private next: Query | null = null;
  private adjTo: number;
  private constraints?: Constraints;
  private operations?: Operations;
  private withClass: string;
  private limit: number;
  private sort?: Sort;
  private list?: string[];
  private groupBySrc: boolean;
  private blobs: boolean;
  private set?: string;
  private vector?: number[];
  private kNeighbors: number;
  private blob?: Buffer;
  private findCommand?: string;

  constructor({
    constraints,
    operations,
    withClass = '',
    limit = -1,
    sort,
    list,
    groupBySrc = false,
    blobs = false,
    adjTo = 0,
    set,
    vector,
    kNeighbors = 0
  }: {
    constraints?: Constraints;
    operations?: Operations;
    withClass?: string;
    limit?: number;
    sort?: Sort;
    list?: string[];
    groupBySrc?: boolean;
    blobs?: boolean;
    adjTo?: number;
    set?: string;
    vector?: number[];
    kNeighbors?: number;
  }) {
    this.constraints = constraints;
    this.operations = operations;
    this.withClass = withClass;
    this.limit = limit;
    this.sort = sort;
    this.list = list;
    this.groupBySrc = groupBySrc;
    this.blobs = blobs;
    this.adjTo = adjTo + 1;
    this.set = set;
    this.vector = vector;
    this.kNeighbors = kNeighbors;
  }

  static spec({
    constraints,
    operations,
    withClass = '',
    limit = -1,
    sort,
    list,
    groupBySrc = false,
    blobs = false,
    set,
    vector,
    kNeighbors = 0
  }: {
    constraints?: Constraints;
    operations?: Operations;
    withClass?: string;
    limit?: number;
    sort?: Sort;
    list?: string[];
    groupBySrc?: boolean;
    blobs?: boolean;
    set?: string;
    vector?: number[];
    kNeighbors?: number;
  }): Query {
    return new Query({
      constraints,
      operations,
      withClass,
      limit,
      sort,
      list,
      groupBySrc,
      blobs,
      set,
      vector,
      kNeighbors
    });
  }

  connectedTo(spec: Query, adjTo: number = 0): Query {
    spec.adjTo = adjTo === 0 ? this.adjTo + 1 : adjTo;
    this.next = spec;
    return this;
  }

  commandProperties(prop: string = ''): any[] {
    const chain: any[] = [];
    let p: Query | null = this;
    while (p !== null) {
      chain.push(prop ? (p as any)[prop] : p);
      p = p.next;
    }
    return chain;
  }

  query(): [QueryCommand[], Buffer[]] {
    const resultsSection = 'results';
    const cmdParams: QueryParams = {
      [resultsSection]: {},
      _ref: this.adjTo
    };

    if (this.limit !== -1) {
      cmdParams[resultsSection]!.limit = this.limit;
    }
    if (this.sort) {
      cmdParams[resultsSection]!.sort = this.sort;
    }
    if (this.list && this.list.length > 0) {
      cmdParams[resultsSection]!.list = this.list;
    } else {
      cmdParams[resultsSection]!.all_properties = true;
    }
    cmdParams[resultsSection]!.group_by_source = this.groupBySrc;

    if (this.constraints) {
      cmdParams.constraints = this.constraints;
    }
    if (this.operations) {
      cmdParams.operations = this.operations;
    }
    if (this.set) {
      cmdParams.set = this.set;
    }
    if (this.kNeighbors > 0) {
      cmdParams.k_neighbors = this.kNeighbors;
    }

    if (this.vector) {
      this.blob = Buffer.from(struct.pack(`${this.vector.length}f`, ...this.vector));
    }

    this.withClass = this.dbObject === 'Entity' ? this.withClass :
      typeof this.dbObject === 'string' ? this.dbObject : this.dbObject;

    const cmd = QueryBuilder.findCommand(this.withClass, cmdParams);
    this.findCommand = Object.keys(cmd)[0];
    const query: QueryCommand[] = [cmd];
    const blobs: Buffer[] = [];

    if (this.blob) {
      blobs.push(this.blob);
    }

    if (this.next) {
      const [nextCommands, nextBlobs] = this.next.query();
      Object.values(nextCommands[0])[0].is_connected_to = {
        ref: this.adjTo
      };
      query.push(...nextCommands);
      blobs.push(...nextBlobs);
    }

    return [query, blobs];
  }
} 