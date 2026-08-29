/** Structured JSON logging. One object per line, so `jq` works on the stream. */

type Level = 'debug' | 'info' | 'warn' | 'error';

const ORDER: Record<Level, number> = { debug: 10, info: 20, warn: 30, error: 40 };
const threshold = ORDER[(process.env.IGNIS_LOG_LEVEL as Level) ?? 'info'] ?? ORDER.info;
const silent = process.env.IGNIS_LOG === 'off';

function emit(level: Level, msg: string, fields: Record<string, unknown>): void {
  if (silent || ORDER[level] < threshold) return;
  const line = JSON.stringify({ ts: new Date().toISOString(), level, msg, ...fields });
  // Logs go to stderr so stdout stays clean for CLI output and shim IPC.
  process.stderr.write(line + '\n');
}

export interface Logger {
  debug(msg: string, fields?: Record<string, unknown>): void;
  info(msg: string, fields?: Record<string, unknown>): void;
  warn(msg: string, fields?: Record<string, unknown>): void;
  error(msg: string, fields?: Record<string, unknown>): void;
  child(bound: Record<string, unknown>): Logger;
}

export function createLogger(bound: Record<string, unknown> = {}): Logger {
  const at = (level: Level) => (msg: string, fields: Record<string, unknown> = {}) =>
    emit(level, msg, { ...bound, ...fields });
  return {
    debug: at('debug'),
    info: at('info'),
    warn: at('warn'),
    error: at('error'),
    child: (extra) => createLogger({ ...bound, ...extra }),
  };
}

export const log = createLogger({ component: 'ignis' });
