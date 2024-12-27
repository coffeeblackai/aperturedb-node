import { config } from 'dotenv';
import { Logger } from './utils/logger.js';

try {
  config();
} catch (error) {
  Logger.warn('Could not load .env file');
}

export function checkQueryResponse(response: any): boolean {
  if (!response) return false;
  
  if (Array.isArray(response)) {
    return response.every(item => checkQueryResponse(item));
  }
  
  if (typeof response === 'object') {
    if ('status' in response) {
      return response.status === 0;
    }
    return Object.values(response).every(value => checkQueryResponse(value));
  }
  
  return false;
}

export function checkQueryResponsePartial(response: any): string[] {
  const warnings: string[] = [];
  
  if (!response) {
    warnings.push('Empty response');
    return warnings;
  }
  
  if (Array.isArray(response)) {
    response.forEach((item, index) => {
      const itemWarnings = checkQueryResponsePartial(item);
      warnings.push(...itemWarnings.map(w => `[${index}] ${w}`));
    });
    return warnings;
  }
  
  if (typeof response === 'object') {
    if ('status' in response) {
      if (response.status !== 0) {
        warnings.push(`Status ${response.status}: ${response.info || 'No info'}`);
      }
    } else {
      Object.entries(response).forEach(([key, value]) => {
        const valueWarnings = checkQueryResponsePartial(value);
        warnings.push(...valueWarnings.map(w => `${key}: ${w}`));
      });
    }
  }
  
  return warnings;
}

export async function executeQuery(client: any, query: any, blobs: Buffer[] = []): Promise<any> {
  Logger.debug('Query=', query);
  try {
    const [response] = await client.query(query, blobs);
    Logger.debug('Response=', response);
    return response;
  } catch (error) {
    if (error instanceof Error) {
      Logger.error(error.message);
    } else {
      Logger.error('Unknown error occurred during query execution');
    }
    throw error;
  }
}

export async function executeQueryWithCheck(client: any, query: any, blobs: Buffer[] = []): Promise<any> {
  const response = await executeQuery(client, query, blobs);
  if (!checkQueryResponse(response)) {
    Logger.error('Failed query =', query, 'with response =', response);
    throw new Error('Query failed');
  }
  return response;
}

export async function executeQueryWithPartialCheck(client: any, query: any, blobs: Buffer[] = []): Promise<[any, string[]]> {
  const response = await executeQuery(client, query, blobs);
  const warnList = checkQueryResponsePartial(response);
  if (warnList.length > 0) {
    Logger.warn('Partial errors:', JSON.stringify(query), JSON.stringify(warnList));
  }
  return [response, warnList];
}

export async function executeQueryWithRetry(client: any, query: any, blobs: Buffer[] = [], maxRetries: number = 3): Promise<any> {
  let lastError: Error | undefined;
  for (let i = 0; i < maxRetries; i++) {
    try {
      return await executeQueryWithCheck(client, query, blobs);
    } catch (error) {
      if (error instanceof Error) {
        lastError = error;
        Logger.error('Query execution error:', error.message);
      } else {
        lastError = new Error('Unknown error during query execution');
        Logger.error('Query execution error: Unknown error');
      }
      if (i < maxRetries - 1) {
        await new Promise(resolve => setTimeout(resolve, 1000 * Math.pow(2, i)));
      }
    }
  }
  throw lastError || new Error('Query failed after retries');
}

export function deprecate(oldName: string, newName: string): void {
  Logger.warn(`${oldName} is deprecated and will be removed in a future release. Use ${newName} instead.`);
} 