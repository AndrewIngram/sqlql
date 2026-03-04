// Browser bundle shim for drizzle-orm/pglite.
// Real PGlite is loaded at runtime from CDN in pglite-runtime.ts.
export class PGlite {
  constructor(..._args: unknown[]) {
    throw new Error("PGlite shim should not be instantiated directly.");
  }
}

export const types = {
  TIMESTAMP: 1114,
  TIMESTAMPTZ: 1184,
  INTERVAL: 1186,
  DATE: 1082,
} as const;
