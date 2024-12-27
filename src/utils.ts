import { ApertureConfig, ApertureError } from './types';
import { BaseClient } from './client/base';
import * as dotenv from 'dotenv';
import * as path from 'path';
import * as fs from 'fs';

/**
 * Create a configuration from a JSON object or string
 */
export function createConfigurationFromJson(
  config: Record<string, any> | string,
  name?: string,
  nameRequired: boolean = false
): ApertureConfig {
  let configObj: Record<string, any>;
  
  if (typeof config === 'string') {
    try {
      configObj = JSON.parse(config);
    } catch (e) {
      throw new Error(`Problem decoding JSON config string: ${e}`);
    }
  } else {
    configObj = config;
  }

  // Remove password before logging
  const cleanConfig = { ...configObj };
  delete cleanConfig.password;

  // Required fields
  if (!configObj.host) throw new Error(`host is required in the configuration: ${JSON.stringify(cleanConfig)}`);
  if (!configObj.username) throw new Error(`username is required in the configuration: ${JSON.stringify(cleanConfig)}`);
  if (!configObj.password) throw new Error(`password is required in the configuration: ${JSON.stringify(cleanConfig)}`);

  // Default values
  if (!configObj.port) configObj.port = 55555;

  if (name) {
    configObj.name = name;
  } else if (nameRequired && !configObj.name) {
    throw new Error(`name is required in the configuration: ${JSON.stringify(cleanConfig)}`);
  } else if (!configObj.name) {
    configObj.name = 'from_json';
  }

  return configObj as ApertureConfig;
}

/**
 * Get a secret from environment variables or .env file
 */
export function getSecret(name: string): string | undefined {
  // First check environment variables
  const envValue = process.env[name];
  if (envValue) return envValue;

  // Then check .env file
  try {
    dotenv.config();
    return process.env[name];
  } catch (error) {
    console.warn('Could not load .env file');
    return undefined;
  }
}

/**
 * Execute a query with proper error handling and logging
 */
export async function executeQuery(
  client: BaseClient,
  query: any[],
  blobs: Buffer[] = [],
  successStatuses: number[] = [0],
  responseHandler?: (query: any[], queryBlobs: Buffer[], response: any[], responseBlobs: Buffer[] | null, cmdIndex?: number) => void,
  commandsPerQuery: number = 1,
  blobsPerQuery: number = 0,
  strictResponseValidation: boolean = false,
  cmdIndex?: number
): Promise<[number, any[], Buffer[]]> {
  let result = 0;
  console.debug('Query=', query);

  try {
    const [response, responseBlobs] = await client.query(query, blobs);
    console.debug('Response=', response);

    if (client.lastQueryOk()) {
      if (responseHandler) {
        try {
          mapResponseToHandler(
            responseHandler,
            query,
            blobs,
            response,
            responseBlobs,
            commandsPerQuery,
            blobsPerQuery,
            cmdIndex
          );
        } catch (error) {
          console.error(error);
          if (strictResponseValidation) {
            throw error;
          }
        }
      }
    } else {
      console.error('Failed query =', query, 'with response =', response);
      result = 1;
    }

    const statuses: Record<number, any[]> = {};
    
    if (!Array.isArray(response)) {
      statuses[response.status] = [response];
    } else {
      response.forEach(res => {
        Object.keys(res).forEach(cmd => {
          const status = res[cmd].status;
          if (!statuses[status]) statuses[status] = [];
          statuses[status].push(res);
        });
      });
    }

    if (result !== 1) {
      const warnList = [];
      for (const [status, results] of Object.entries(statuses)) {
        if (!successStatuses.includes(Number(status))) {
          warnList.push(...results);
        }
      }
      if (warnList.length > 0) {
        console.warn('Partial errors:', JSON.stringify(query), JSON.stringify(warnList));
        result = 2;
      }
    }

    return [result, response, responseBlobs];
  } catch (error) {
    console.error('Query execution error:', error);
    throw error;
  }
}

/**
 * Map response to handler function
 */
function mapResponseToHandler(
  handler: (query: any[], queryBlobs: Buffer[], response: any[], responseBlobs: Buffer[] | null, cmdIndex?: number) => void,
  query: any[],
  queryBlobs: Buffer[],
  response: any[],
  responseBlobs: Buffer[],
  commandsPerQuery: number,
  blobsPerQuery: number,
  cmdIndexOffset?: number
): void {
  let blobsReturned = 0;

  for (let i = 0; i < Math.ceil(query.length / commandsPerQuery); i++) {
    const start = i * commandsPerQuery;
    const end = start + commandsPerQuery;
    const blobsStart = i * blobsPerQuery;
    const blobsEnd = blobsStart + blobsPerQuery;

    let bCount = 0;
    if (Array.isArray(response)) {
      query.slice(start, end).forEach((req, idx) => {
        const resp = response[start + idx];
        Object.keys(req).forEach(k => {
          const blobReturningCommands = ['FindImage', 'FindBlob', 'FindVideo', 'FindDescriptor', 'FindBoundingBox'];
          if (blobReturningCommands.includes(k) && req[k].blobs) {
            bCount += resp[k].returned;
          }
        });
      });
    }

    handler(
      query.slice(start, end),
      queryBlobs.slice(blobsStart, blobsEnd),
      Array.isArray(response) ? response.slice(start, end) : response,
      responseBlobs.slice(blobsReturned, blobsReturned + bCount),
      cmdIndexOffset !== undefined ? cmdIndexOffset + i : undefined
    );

    blobsReturned += bCount;
  }
}

/**
 * Issue a deprecation warning
 */
export function issueDeprecationWarning(oldName: string, newName: string): void {
  console.warn(`${oldName} is deprecated and will be removed in a future release. Use ${newName} instead.`);
} 