import { Logger } from '@nestjs/common';
import { setServers as setDefaultResolverServers } from 'dns';
import * as dns from 'dns/promises';

const logger = new Logger('MongoSrv');

// Public resolvers used only when the OS resolver cannot answer SRV. On Windows,
// c-ares can fall back to a loopback resolver (e.g. the ICS DNS proxy on 127.0.0.1)
// that refuses queries with ECONNREFUSED, breaking mongodb+srv resolution.
// Ported from DaPlex-API src/common/database/mongo-connectivity.ts.
const PUBLIC_DNS_FALLBACK = ['1.1.1.1', '8.8.8.8'];

/** Extract the cluster hostname from a mongodb / mongodb+srv URI (no port). */
export function parseSrvHost(uri: string): string | null {
  const match = uri.match(/^mongodb(?:\+srv)?:\/\/(?:[^@/]+@)?([^/?,]+)/i);
  if (!match) return null;
  return match[1].split(':')[0];
}

/** True for a resolver address that cannot reach the public internet (loopback / unspecified). */
function isLoopbackResolver(server: string): boolean {
  // dns.getServers() may append a port: "1.1.1.1:5353" (v4) or "[::1]:5353" (v6).
  let host = server;
  const bracketed = host.match(/^\[(.+)\](?::\d+)?$/);
  if (bracketed) {
    host = bracketed[1];
  } else if ((host.match(/:/g) || []).length === 1) {
    host = host.split(':')[0];
  }
  return host === '::1' || host === '0.0.0.0' || host.startsWith('127.');
}

/** True when every configured resolver is loopback/unspecified (or there are none). */
function isLoopbackOnly(servers: string[]): boolean {
  return servers.length === 0 || servers.every(isLoopbackResolver);
}

/**
 * Make sure Node can resolve the cluster's SRV record before the driver tries to.
 * If the configured resolver is loopback-only, or it cannot answer the SRV query,
 * switch the process to public resolvers. No-op when the OS resolver already works.
 * Idempotent; never throws.
 */
export async function ensureSrvResolvable(srvHost: string): Promise<void> {
  const servers = dns.getServers();

  if (!isLoopbackOnly(servers)) {
    try {
      await dns.resolveSrv(`_mongodb._tcp.${srvHost}`);
      return; // the configured resolver works; leave it alone
    } catch (error) {
      logger.warn(
        `SRV lookup failed via configured DNS (${servers.join(', ') || 'none'}): ` +
          `${(error as Error).message}; switching to public resolvers.`
      );
    }
  } else {
    logger.warn(
      `Node DNS resolver is loopback-only (${servers.join(', ') || 'none'}) — likely a local ` +
        'DNS proxy (e.g. Windows ICS) that breaks SRV resolution; switching MongoDB to public resolvers.'
    );
  }

  try {
    // Set both the promises and the callback default resolvers — they can diverge,
    // and the mongodb driver may use either depending on the Node/driver version.
    dns.setServers(PUBLIC_DNS_FALLBACK);
    setDefaultResolverServers(PUBLIC_DNS_FALLBACK);
    await dns.resolveSrv(`_mongodb._tcp.${srvHost}`);
    logger.log(`DNS fallback active: resolving via ${PUBLIC_DNS_FALLBACK.join(', ')}.`);
  } catch (error) {
    logger.error(`SRV lookup still failing after switching to public resolvers: ${(error as Error).message}`);
  }
}
