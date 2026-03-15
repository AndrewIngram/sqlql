import { stringifyUnknownValue, type QueryRow, type RelNode } from "@tupl/foundation";

import {
  normalizeCapability,
  unwrapProviderOperationResult,
  type FragmentProviderAdapter,
  type ProviderCapabilityReport,
} from "./index";

export interface ProviderConformanceCase {
  name: string;
  run(): Promise<void>;
}

export interface ProviderConformanceOptions<
  TContext,
  TProvider extends FragmentProviderAdapter<TContext>,
  TBinding = void,
> {
  provider: TProvider | ((binding: TBinding) => TProvider);
  providerBinding?: TBinding;
  context: TContext;
  rel: {
    node: RelNode;
    expectedRows: QueryRow[];
  };
}

/**
 * Provider testing owns a framework-neutral conformance surface for adapter authors.
 * It stays intentionally small: callers provide their own provider fixture and runtime binding.
 */
export function createProviderConformanceCases<
  TContext,
  TProvider extends FragmentProviderAdapter<TContext>,
  TBinding = void,
>(options: ProviderConformanceOptions<TContext, TProvider, TBinding>): ProviderConformanceCase[] {
  const resolveProvider = () =>
    typeof options.provider === "function"
      ? (options.provider as (binding: TBinding) => TProvider)(options.providerBinding as TBinding)
      : options.provider;

  return [
    {
      name: "executes relational fragments",
      async run() {
        const provider = resolveProvider();
        await assertRelSupported(provider, options.rel.node, options.context);
        const rows = await executeRel(provider, options.rel.node, options.context);
        assertRowsEqual("rel fragment", rows, options.rel.expectedRows);
      },
    },
    {
      name: "preserves projected row shape for relational fragments",
      async run() {
        const provider = resolveProvider();
        const rows = await executeRel(provider, options.rel.node, options.context);
        const expectedKeys =
          Object.keys(options.rel.expectedRows[0] ?? {}).length > 0
            ? Object.keys(options.rel.expectedRows[0] ?? {})
            : options.rel.node.output.map((column) => column.name);

        for (const row of rows) {
          const actualKeys = Object.keys(row);
          if (actualKeys.length !== expectedKeys.length) {
            throw new Error(
              `Rel fragment returned unexpected projection width. Expected ${expectedKeys.join(", ")}, got ${actualKeys.join(", ")}.`,
            );
          }

          for (const [index, key] of expectedKeys.entries()) {
            if (actualKeys[index] !== key) {
              throw new Error(
                `Rel fragment returned unexpected projection order. Expected ${expectedKeys.join(", ")}, got ${actualKeys.join(", ")}.`,
              );
            }
          }
        }
      },
    },
  ];
}

async function assertRelSupported<TContext>(
  provider: FragmentProviderAdapter<TContext>,
  rel: RelNode,
  context: TContext,
) {
  const capability = normalizeCapability(await Promise.resolve(provider.canExecute(rel, context)));
  if (!capabilitySupported(capability)) {
    throw new Error(
      `Provider ${provider.name} reported unsupported rel: ${describeCapability(capability)}.`,
    );
  }
}

async function executeRel<TContext>(
  provider: FragmentProviderAdapter<TContext>,
  rel: RelNode,
  context: TContext,
): Promise<QueryRow[]> {
  const compiled = unwrapProviderOperationResult(
    await Promise.resolve(provider.compile(rel, context)),
  );
  return unwrapProviderOperationResult(await Promise.resolve(provider.execute(compiled, context)));
}

function capabilitySupported(capability: boolean | ProviderCapabilityReport): boolean {
  return typeof capability === "boolean" ? capability : capability.supported;
}

function describeCapability(capability: boolean | ProviderCapabilityReport): string {
  if (typeof capability === "boolean") {
    return capability ? "supported" : "unsupported";
  }
  if (capability.reason) {
    return capability.reason;
  }
  return "unsupported";
}

function assertRowsEqual(label: string, actual: QueryRow[], expected: QueryRow[]) {
  const actualJson = serializeRows(actual);
  const expectedJson = serializeRows(expected);
  if (actualJson !== expectedJson) {
    throw new Error(`${label} rows differed.\nExpected: ${expectedJson}\nActual: ${actualJson}`);
  }
}

function serializeRows(rows: QueryRow[]): string {
  return JSON.stringify(
    rows.map((row) =>
      Object.fromEntries(
        Object.entries(row).map(([key, value]) => [key, stringifyUnknownValue(value)]),
      ),
    ),
  );
}
