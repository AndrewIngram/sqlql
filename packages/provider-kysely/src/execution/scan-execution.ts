import { TuplExecutionError, TuplProviderBindingError } from "@tupl/foundation";
import {
  AdapterResult,
  type ProviderOperationResult,
  type QueryRow,
  type TableScanRequest,
} from "@tupl/provider-kit";
import type { KyselyDatabaseLike, ResolvedEntityConfig, ScanBinding } from "../types";
import { applyBase, applyWhereClause } from "../backend/query-helpers";

function normalizeKyselyScanError(
  error: unknown,
  table: string,
): TuplProviderBindingError | TuplExecutionError {
  if (error instanceof TuplProviderBindingError || error instanceof TuplExecutionError) {
    return error;
  }

  if (error instanceof Error) {
    return new TuplExecutionError({
      operation: "execute Kysely scan",
      message: error.message,
      cause: error,
    });
  }

  return new TuplExecutionError({
    operation: "execute Kysely scan",
    message: `Failed to execute Kysely scan for "${table}".`,
    cause: error,
  });
}

export async function executeScanResult<TContext>(
  db: KyselyDatabaseLike,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
  request: TableScanRequest,
  context: TContext,
): Promise<ProviderOperationResult<QueryRow[], TuplProviderBindingError | TuplExecutionError>> {
  const binding = entityConfigs[request.table];
  if (!binding) {
    return AdapterResult.err(
      new TuplProviderBindingError({
        provider: "kysely",
        table: request.table,
        message: `Unknown Kysely entity config: ${request.table}`,
      }),
    );
  }

  return AdapterResult.tryPromise({
    try: async () => {
      const alias = request.alias ?? binding.table;
      const from = `${binding.table} as ${alias}`;

      let query = db.selectFrom(from);
      query = await applyBase(query, db, binding, context, alias);

      const aliases = new Map<string, ScanBinding<TContext>>([
        [
          alias,
          {
            alias,
            entity: binding.entity,
            table: binding.table,
            scan: {
              id: "scan",
              kind: "scan",
              convention: "local",
              table: binding.entity,
              ...(request.alias ? { alias: request.alias } : {}),
              select: request.select,
              ...(request.where ? { where: request.where } : {}),
              output: [],
            },
            resolved: binding,
          },
        ],
      ]);

      for (const clause of request.where ?? []) {
        query = applyWhereClause(query, clause, aliases);
      }

      for (const term of request.orderBy ?? []) {
        query = query.orderBy(`${alias}.${term.column}`, term.direction);
      }

      if (request.limit != null) {
        query = query.limit(request.limit);
      }
      if (request.offset != null) {
        query = query.offset(request.offset);
      }

      query = query.select((eb: any) =>
        request.select.map((column) => eb.ref(`${alias}.${column}`).as(column)),
      );

      return await query.execute();
    },
    catch: (error) => normalizeKyselyScanError(error, request.table),
  });
}

export async function executeScan<TContext>(
  db: KyselyDatabaseLike,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
  request: TableScanRequest,
  context: TContext,
): Promise<QueryRow[]> {
  return (await executeScanResult(db, entityConfigs, request, context)).unwrap();
}
