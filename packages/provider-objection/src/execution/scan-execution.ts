import type { QueryRow, TableScanRequest } from "@tupl/provider-kit";

import { applyWhereClause, createBaseQuery, executeQuery } from "../backend/query-helpers";
import type { ResolvedEntityConfig } from "../types";
import type { ScanBinding } from "../planning/rel-strategy";

export async function executeScan<TContext>(
  knex: import("../types").KnexLike,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
  request: TableScanRequest,
  context: TContext,
): Promise<QueryRow[]> {
  const binding = entityConfigs[request.table];
  if (!binding) {
    throw new Error(`Unknown Objection entity config: ${request.table}`);
  }

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
        config: binding.config,
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

  return executeQuery(query);
}
