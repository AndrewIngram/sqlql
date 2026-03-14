import type { QueryRow, TableScanRequest } from "@tupl/provider-kit";
import type { KyselyDatabaseLike, ResolvedEntityConfig } from "../types";
import { applyBase, applyWhereClause } from "../backend/query-helpers";
import type { ScanBinding } from "../planning/rel-strategy";

export async function executeScan<TContext>(
  db: KyselyDatabaseLike,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
  request: TableScanRequest,
  context: TContext,
): Promise<QueryRow[]> {
  const binding = entityConfigs[request.table];
  if (!binding) {
    throw new Error(`Unknown Kysely entity config: ${request.table}`);
  }

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

  return query.execute();
}
