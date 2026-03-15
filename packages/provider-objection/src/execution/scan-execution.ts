import { TuplExecutionError, TuplProviderBindingError } from "@tupl/foundation";
import {
  AdapterResult,
  type ProviderOperationResult,
  type QueryRow,
  type TableScanRequest,
} from "@tupl/provider-kit";

import { applyWhereClause, createBaseQuery, executeQuery } from "../backend/query-helpers";
import type { ResolvedEntityConfig, ScanBinding } from "../types";

function normalizeObjectionScanError(
  error: unknown,
  table: string,
): TuplProviderBindingError | TuplExecutionError {
  if (error instanceof TuplProviderBindingError || error instanceof TuplExecutionError) {
    return error;
  }

  if (error instanceof Error) {
    if (
      error.message ===
      "Objection entity base(context) must return a Knex/Objection query builder synchronously."
    ) {
      return new TuplProviderBindingError({
        provider: "objection",
        table,
        message: error.message,
        cause: error,
      });
    }

    return new TuplExecutionError({
      operation: "execute Objection scan",
      message: error.message,
      cause: error,
    });
  }

  return new TuplExecutionError({
    operation: "execute Objection scan",
    message: `Failed to execute Objection scan for "${table}".`,
    cause: error,
  });
}

export async function executeScanResult<TContext>(
  knex: import("../types").KnexLike,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
  request: TableScanRequest,
  context: TContext,
): Promise<ProviderOperationResult<QueryRow[], TuplProviderBindingError | TuplExecutionError>> {
  const binding = entityConfigs[request.table];
  if (!binding) {
    return AdapterResult.err(
      new TuplProviderBindingError({
        provider: "objection",
        table: request.table,
        message: `Unknown Objection entity config: ${request.table}`,
      }),
    );
  }

  return AdapterResult.tryPromise({
    try: async () => {
      const alias = request.alias ?? binding.table;
      let query = createBaseQuery(knex, binding, context, request.alias);

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
        query = query.orderBy(`${request.alias ?? binding.table}.${term.column}`, term.direction);
      }

      if (request.limit != null) {
        query = query.limit(request.limit);
      }
      if (request.offset != null) {
        query = query.offset(request.offset);
      }

      query = query.clearSelect?.() ?? query;
      for (const column of request.select) {
        query = query.select({ [column]: `${request.alias ?? binding.table}.${column}` });
      }

      return await executeQuery(query);
    },
    catch: (error) => normalizeObjectionScanError(error, request.table),
  });
}

export async function executeScan<TContext>(
  knex: import("../types").KnexLike,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
  request: TableScanRequest,
  context: TContext,
): Promise<QueryRow[]> {
  return (await executeScanResult(knex, entityConfigs, request, context)).unwrap();
}
