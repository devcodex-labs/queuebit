import { createClient, createSentinel, ErrorReply } from '@redis/client';
import type { RedisClientOptions } from '@redis/client';
import type { RedisOptions, TlsOptions } from '../../api/types.js';
import { QueuebitError } from '../../api/errors.js';

interface OwnedClient {
  readonly isOpen: boolean; readonly isReady: boolean;
  connect(): Promise<unknown>; send(args: string[], signal: AbortSignal, timeout: number): Promise<unknown>;
  destroy(): void | Promise<void>;
}

export interface TransportBudget { readonly deadline: number; retriesRemaining: number }
export function transportBudget(deadline = Date.now() + 10000): TransportBudget {
  return { deadline, retriesRemaining: 2 };
}

function socket(tls?: TlsOptions): NonNullable<RedisClientOptions['socket']> {
  return tls === undefined ? { connectTimeout: 5000, reconnectStrategy: false }
    : { ...tls, tls: true, rejectUnauthorized: true, connectTimeout: 5000, reconnectStrategy: false };
}

function ownedClient(options: RedisOptions, onError: () => void): OwnedClient {
  // Raw storage decoding is explicitly RESP2; node-redis 6 otherwise defaults to RESP3 maps.
  const common = { RESP: 2 as const, disableOfflineQueue: true, commandsQueueMaxLength: 128 };
  if (options.mode === 'sentinel') {
    const client = createSentinel({ RESP: 2, name: options.name, sentinelRootNodes: options.seeds,
      masterPoolSize: 1, replicaPoolSize: 0, maxCommandRediscovers: 0, passthroughClientErrorEvents: true,
      nodeClientOptions: { ...common, ...options.nodeAuth, database: options.database ?? 0, socket: socket(options.nodeTls) },
      sentinelClientOptions: { ...common, ...options.sentinelAuth, socket: socket(options.sentinelTls) },
      ...(options.addressMap === undefined ? {} : { nodeAddressMap: options.addressMap }) });
    client.on('error', onError);
    return { get isOpen() { return client.isOpen; }, get isReady() { return client.isReady; },
      connect: () => client.connect(), send: (args, abortSignal, timeout) => client.sendCommand(false, args, { abortSignal, timeout }),
      destroy: () => client.destroy() };
  }
  const client = options.mode === 'url'
    ? createClient({ ...common, url: options.url, socket: socket(options.tls ?? (options.url.startsWith('rediss:') ? {} : undefined)) })
    : createClient({ ...common, ...(options.username === undefined ? {} : { username: options.username }),
      ...(options.password === undefined ? {} : { password: options.password }), database: options.database ?? 0,
      socket: { ...socket(options.tls), host: options.host, port: options.port } });
  client.on('error', onError);
  return { get isOpen() { return client.isOpen; }, get isReady() { return client.isReady; },
    connect: () => client.connect(), send: (args, abortSignal, timeout) => client.sendCommand(args, { abortSignal, timeout }),
    destroy: () => client.destroy() };
}

/** A timer bounds topology/pool waits too. Both late resolve and rejection remain observed. */
export async function beforeDeadline<T>(promise: Promise<T>, deadline: number, onTimeout: () => void): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => { onTimeout(); reject(new QueuebitError('CONNECTION_UNAVAILABLE', 'Redis operation exceeded its deadline', { retryable: true })); }, Math.max(0, deadline - Date.now()));
    })]);
  } finally { if (timer !== undefined) clearTimeout(timer); }
}

/** One owned connection/rebuild at a time; construction does not call the SDK or start timers. */
export class BatchRedisConnection {
  readonly options: RedisOptions;
  #client: OwnedClient | undefined;
  #connecting: Promise<void> | undefined;
  #closing: Promise<void> | undefined;
  #destroying: Promise<void> | undefined;
  #closed = false;
  #failed = false;
  #generation = 0;
  #lastErrorAt: number | null = null;
  constructor(options: RedisOptions) { this.options = options; }
  get ready(): boolean { return !this.#closed && !this.#failed && (this.#client?.isReady ?? false); }
  get generation(): number { return this.#generation; }
  get lastErrorAt(): number | null { return this.#lastErrorAt; }

  connect(deadline = Date.now() + 5000): Promise<void> {
    if (this.#closed || this.#failed) return Promise.reject(new QueuebitError('QUEUE_FAILED', 'Redis connection is permanently unavailable'));
    if (this.#connecting) return beforeDeadline(this.#connecting, deadline, () => {});
    if (this.ready) return Promise.resolve();
    const generation = ++this.#generation;
    this.#connecting = this.#open(generation, Math.min(deadline, Date.now() + 5000)).finally(() => { this.#connecting = undefined; });
    return this.#connecting;
  }
  async #open(generation: number, deadline: number): Promise<void> {
    if (this.#client) await this.#destroy(deadline);
    if (this.#closed || this.#generation !== generation) throw new QueuebitError('QUEUE_CLOSING', 'Startup generation was revoked');
    const client = ownedClient(this.options, () => { this.#lastErrorAt = Date.now(); });
    this.#client = client;
    try {
      await beforeDeadline(client.connect(), deadline, () => { this.#generation++; });
      if (this.#closed || this.#generation !== generation) throw new QueuebitError('QUEUE_CLOSING', 'Startup generation was revoked');
    } catch (error) {
      await this.#destroy(Math.max(Date.now(), deadline));
      if (error instanceof QueuebitError) throw error;
      throw new QueuebitError('CONNECTION_UNAVAILABLE', 'Could not connect to Redis', { retryable: true, operation: 'connect' });
    }
  }
  #destroy(deadline: number, expected?: OwnedClient): Promise<void> {
    if (this.#destroying) return beforeDeadline(this.#destroying, deadline, () => { this.#failed = true; });
    const client = this.#client;
    if (!client || (expected !== undefined && client !== expected)) return Promise.resolve();
    this.#destroying = this.#destroyClient(client, deadline).finally(() => { this.#destroying = undefined; });
    return this.#destroying;
  }
  async #destroyClient(client: OwnedClient, deadline: number): Promise<void> {
    try {
      // A failed handshake may already have closed itself; destroy() then throws in node-redis.
      if (client.isOpen) await beforeDeadline(Promise.resolve(client.destroy()), deadline, () => { this.#failed = true; });
      if (client.isOpen) throw new Error('Owned client still open');
      if (this.#client === client) this.#client = undefined;
    } catch {
      this.#failed = true;
      throw new QueuebitError('RESOURCE_CLEANUP_FAILED', 'Redis resource cleanup could not be verified', { operation: 'close' });
    }
  }

  /** A send failure does not prove zero writes. This layer never retries a handler or changes command identity. */
  async command(args: string[], context: { write?: boolean; runId?: string; commandId?: string; deadline?: number; budget?: TransportBudget } = {}): Promise<unknown> {
    const budget = context.budget ?? transportBudget(context.deadline);
    const deadline = Math.min(budget.deadline, context.deadline ?? Date.now() + 10000, Date.now() + 10000);
    let possiblySent = false;
    for (let attempt = 0; attempt < 3 && Date.now() < deadline; attempt++) {
      const abort = new AbortController();
      let sendingClient: OwnedClient | undefined;
      try {
        await beforeDeadline(this.connect(deadline), deadline, () => {});
        if (!this.#client?.isReady || this.#closed) throw new QueuebitError('CONNECTION_UNAVAILABLE', 'Redis is not ready', { retryable: true });
        if (Date.now() >= deadline) break;
        const timeout = Math.max(1, Math.min(5000, deadline - Date.now()));
        sendingClient = this.#client;
        possiblySent = true;
        return await beforeDeadline(sendingClient.send(args, abort.signal, timeout), Date.now() + timeout, () => { abort.abort(); });
      } catch (error) {
        abort.abort();
        if (error instanceof ErrorReply) throw error;
        if (error instanceof QueuebitError && ['RESOURCE_CLEANUP_FAILED', 'QUEUE_CLOSING', 'QUEUE_FAILED'].includes(error.code)) break;
        try { await this.#destroy(deadline, sendingClient); } catch { break; }
        if (budget.retriesRemaining <= 0) break;
        budget.retriesRemaining--;
      }
    }
    if (context.write && possiblySent) throw new QueuebitError('OUTCOME_UNKNOWN', 'Redis write outcome could not be determined', {
      operation: 'redis', outcomeKnown: false, retryable: true,
      ...(context.runId === undefined ? {} : { runId: context.runId }), ...(context.commandId === undefined ? {} : { commandId: context.commandId }) });
    throw new QueuebitError('CONNECTION_UNAVAILABLE', 'Redis operation failed within its transport budget', { operation: 'redis', retryable: !this.#failed });
  }
  close(deadline = Date.now() + 10000): Promise<void> {
    if (this.#closing) return this.#closing;
    this.#closed = true;
    this.#generation++;
    this.#closing = (async () => {
      await this.#destroy(deadline);
      if (this.#connecting) {
        await beforeDeadline(this.#connecting.catch(() => undefined), deadline, () => { this.#failed = true; });
        await this.#destroy(deadline);
      }
    })();
    return this.#closing;
  }
}
