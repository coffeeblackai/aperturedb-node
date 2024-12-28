import { ApertureClient } from '../../index.js';
import { BaseClient } from '../../base.js';
import type { ApertureConfig } from '../../types.js';
import { LogLevel } from '../../utils/logger.js';

let client: ApertureClient | null = null;

export function getTestClient(config?: Partial<ApertureConfig>): ApertureClient {
  if (!client) {
    const defaultConfig: ApertureConfig = {
      host: process.env.APERTURE_HOST!,
      port: 55555,
      username: process.env.APERTURE_USER!,
      password: process.env.APERTURE_PASSWORD!,
      useSsl: true,
      useKeepalive: true,
      retryIntervalSeconds: 5,
      retryMaxAttempts: 1,
      ...config
    };

    client = ApertureClient.getInstance(defaultConfig);
    client.setLogLevel(LogLevel.TRACE);
  }
  return client;
}

export async function destroyTestClient(): Promise<void> {
  if (client) {
    await client.destroy();
    client = null;
  }
  // Clean up connection pool
  (BaseClient as any).ConnectionPool?.destroyInstance();
}

export async function resetTestClient(): Promise<void> {
  await destroyTestClient();
  (ApertureClient as any)['instance'] = null; // Reset singleton for testing
} 