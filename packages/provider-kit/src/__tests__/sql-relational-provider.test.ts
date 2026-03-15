import { describe, expect, it } from "vitest";

import {
  buildSqlRelationalQueryForStrategy,
  createSqlRelationalProviderAdapter,
  type QueryRow,
  type RelationalProviderEntityConfig,
  type SqlRelationalQueryTranslationBackend,
  unwrapProviderOperationResult,
} from "@tupl/provider-kit";

type FakeEntityConfig = RelationalProviderEntityConfig & { table?: string };
type FakeQuery = { steps: string[] };

const fakePlanning = {
  createScanBinding(scan: Extract<any, { kind: "scan" }>, resolvedEntities: Record<string, any>) {
    const resolved = resolvedEntities[scan.table];
    if (!resolved) {
      throw new Error(`missing entity ${scan.table}`);
    }
    return {
      alias: scan.alias ?? resolved.table,
      entity: resolved.entity,
      table: resolved.table,
      scan,
      resolved,
    };
  },
};

const fakeTranslationBackend: SqlRelationalQueryTranslationBackend<
  unknown,
  any,
  any,
  unknown,
  FakeQuery
> = {
  createRootQuery({ root }: { root: { alias: string } }): FakeQuery {
    return { steps: [`root:${root.alias}`] };
  },
  applyRegularJoin({
    query,
    join,
  }: {
    query: FakeQuery;
    join: { joinType: string; right: { alias: string } };
  }): FakeQuery {
    return { steps: [...query.steps, `join:${join.joinType}:${join.right.alias}`] };
  },
  applySemiJoin({
    query,
    leftKey,
  }: {
    query: FakeQuery;
    leftKey: { alias: string; column: string };
  }): FakeQuery {
    return { steps: [...query.steps, `semi:${leftKey.alias}.${leftKey.column}`] };
  },
  applyWhereClause({
    query,
    clause,
  }: {
    query: FakeQuery;
    clause: { column: string; op: string };
  }): FakeQuery {
    return { steps: [...query.steps, `where:${clause.column}:${clause.op}`] };
  },
  applySelection({
    query,
    selection,
  }: {
    query: FakeQuery;
    selection: Array<{ kind: string; output: string }>;
  }): FakeQuery {
    return {
      steps: [
        ...query.steps,
        `select:${selection.map((entry) => `${entry.kind}:${entry.output}`).join(",")}`,
      ],
    };
  },
  applyGroupBy({
    query,
    groupBy,
  }: {
    query: FakeQuery;
    groupBy: Array<{ alias?: string; table?: string; column: string }>;
  }): FakeQuery {
    return {
      steps: [
        ...query.steps,
        `group:${groupBy.map((entry) => `${entry.alias ?? entry.table}.${entry.column}`).join(",")}`,
      ],
    };
  },
  applyOrderBy({
    query,
    orderBy,
  }: {
    query: FakeQuery;
    orderBy: Array<{ kind: string; column?: string; source?: { column: string } }>;
  }): FakeQuery {
    return {
      steps: [
        ...query.steps,
        `order:${orderBy
          .map((entry) => (entry.kind === "output" ? entry.column : entry.source?.column))
          .join(",")}`,
      ],
    };
  },
  applyLimit({ query, limit }: { query: FakeQuery; limit: number }): FakeQuery {
    return { steps: [...query.steps, `limit:${limit}`] };
  },
  applyOffset({ query, offset }: { query: FakeQuery; offset: number }): FakeQuery {
    return { steps: [...query.steps, `offset:${offset}`] };
  },
  applySetOp({
    left,
    right,
    wrapper,
  }: {
    left: FakeQuery;
    right: FakeQuery;
    wrapper: { setOp: { op: string } };
  }): FakeQuery {
    return {
      steps: [...left.steps, `set:${wrapper.setOp.op}`, ...right.steps],
    };
  },
  buildWithQuery({
    ctes,
    projection,
    orderBy,
  }: {
    ctes: Array<{ name: string }>;
    projection: Array<{ kind: string; output: string }>;
    orderBy: Array<{ kind: string; column?: string; source?: { column: string } }>;
  }): FakeQuery {
    return {
      steps: [
        `with:${ctes.map((cte) => cte.name).join(",")}`,
        `with-select:${projection.map((entry) => `${entry.kind}:${entry.output}`).join(",")}`,
        `with-order:${orderBy
          .map((entry) => (entry.kind === "output" ? entry.column : entry.source?.column))
          .join(",")}`,
      ],
    };
  },
  async executeQuery({ query }: { query: FakeQuery }): Promise<QueryRow[]> {
    return [{ steps: query.steps.join(" > ") }];
  },
};

function createFakeProvider() {
  const entities = {
    orders: { shape: { id: "text", total_cents: "integer" } },
    archived_orders: { shape: { id: "text", total_cents: "integer" } },
  } satisfies Record<string, FakeEntityConfig>;

  return createSqlRelationalProviderAdapter({
    name: "warehouse",
    entities,
    queryBackend: {
      buildQueryForStrategy({ rel, strategy, resolvedEntities, runtime, context, compileOptions }) {
        return buildSqlRelationalQueryForStrategy(
          rel,
          strategy,
          resolvedEntities,
          fakeTranslationBackend,
          runtime,
          context,
          {
            createScanBinding: (scan, resolvedEntities) =>
              fakePlanning.createScanBinding(scan, resolvedEntities),
          },
          compileOptions,
        );
      },
      async executeQuery({ query }) {
        return fakeTranslationBackend.executeQuery({ query, context: {}, runtime: {} });
      },
    },
    resolveRuntime() {
      return {};
    },
  });
}

describe("sql relational provider factory", () => {
  it("defaults resolved entity tables from config.table or entity name", async () => {
    const provider = createSqlRelationalProviderAdapter({
      name: "warehouse",
      entities: {
        orders: { table: "orders_raw", shape: { id: "text" } },
      },
      queryBackend: {
        buildQueryForStrategy({
          rel,
          strategy,
          resolvedEntities,
          runtime,
          context,
          compileOptions,
        }) {
          return buildSqlRelationalQueryForStrategy(
            rel,
            strategy,
            resolvedEntities,
            fakeTranslationBackend,
            runtime,
            context,
            {
              createScanBinding: (scan, resolvedEntities) =>
                fakePlanning.createScanBinding(scan, resolvedEntities),
            },
            compileOptions,
          );
        },
        async executeQuery({ query }) {
          return fakeTranslationBackend.executeQuery({ query, context: {}, runtime: {} });
        },
      },
      resolveRuntime() {
        return {};
      },
    });

    const compiled = unwrapProviderOperationResult(
      await provider.compile(
        {
          id: "scan",
          kind: "scan",
          convention: "local",
          table: "orders",
          select: ["id"],
          output: [{ name: "id" }],
        },
        {},
      ),
    );
    const rows = unwrapProviderOperationResult(await provider.execute(compiled, {}));

    expect(rows).toEqual([{ steps: "root:orders_raw > select:column:orders_raw.id" }]);
  });

  it("executes shared basic single-query pushdown through the public factory", async () => {
    const provider = createFakeProvider();
    const compiled = unwrapProviderOperationResult(
      await provider.compile(
        {
          id: "limit",
          kind: "limit_offset",
          convention: "local",
          limit: 5,
          input: {
            id: "sort",
            kind: "sort",
            convention: "local",
            orderBy: [{ source: { column: "id" }, direction: "asc" }],
            input: {
              id: "project",
              kind: "project",
              convention: "local",
              columns: [{ source: { alias: "orders", column: "id" }, output: "id" }],
              input: {
                id: "scan",
                kind: "scan",
                convention: "local",
                table: "orders",
                alias: "orders",
                select: ["id"],
                where: [{ column: "id", op: "is_not_null" }],
                output: [{ name: "orders.id" }],
              },
              output: [{ name: "id" }],
            },
            output: [{ name: "id" }],
          },
          output: [{ name: "id" }],
        },
        {},
      ),
    );
    const rows = unwrapProviderOperationResult(await provider.execute(compiled, {}));

    expect(rows).toEqual([
      {
        steps: "root:orders > where:id:is_not_null > select:column:id > order:id > limit:5",
      },
    ]);
  });

  it("handles set-op and with recursion without provider-specific rel planning code", async () => {
    const provider = createFakeProvider();

    const setOpCompiled = unwrapProviderOperationResult(
      await provider.compile(
        {
          id: "setop",
          kind: "set_op",
          convention: "local",
          op: "union_all",
          left: {
            id: "left_project",
            kind: "project",
            convention: "local",
            columns: [{ source: { alias: "orders", column: "id" }, output: "id" }],
            input: {
              id: "left_scan",
              kind: "scan",
              convention: "local",
              table: "orders",
              alias: "orders",
              select: ["id"],
              output: [{ name: "orders.id" }],
            },
            output: [{ name: "id" }],
          },
          right: {
            id: "right_project",
            kind: "project",
            convention: "local",
            columns: [{ source: { alias: "archived_orders", column: "id" }, output: "id" }],
            input: {
              id: "right_scan",
              kind: "scan",
              convention: "local",
              table: "archived_orders",
              alias: "archived_orders",
              select: ["id"],
              output: [{ name: "archived_orders.id" }],
            },
            output: [{ name: "id" }],
          },
          output: [{ name: "id" }],
        },
        {},
      ),
    );
    const setOpRows = unwrapProviderOperationResult(await provider.execute(setOpCompiled, {}));

    expect(setOpRows).toEqual([
      {
        steps:
          "root:orders > select:column:id > set:union_all > root:archived_orders > select:column:id",
      },
    ]);

    const withCompiled = unwrapProviderOperationResult(
      await provider.compile(
        {
          id: "with",
          kind: "with",
          convention: "local",
          ctes: [
            {
              name: "recent_orders",
              query: {
                id: "cte_project",
                kind: "project",
                convention: "local",
                columns: [{ source: { alias: "orders", column: "id" }, output: "id" }],
                input: {
                  id: "cte_scan",
                  kind: "scan",
                  convention: "local",
                  table: "orders",
                  alias: "orders",
                  select: ["id"],
                  output: [{ name: "orders.id" }],
                },
                output: [{ name: "id" }],
              },
            },
          ],
          body: {
            id: "body_sort",
            kind: "sort",
            convention: "local",
            orderBy: [{ source: { column: "id" }, direction: "asc" }],
            input: {
              id: "body_project",
              kind: "project",
              convention: "local",
              columns: [{ source: { alias: "recent_orders", column: "id" }, output: "id" }],
              input: {
                id: "body_cte_ref",
                kind: "cte_ref",
                convention: "local",
                name: "recent_orders",
                alias: "recent_orders",
                select: ["id"],
                output: [{ name: "recent_orders.id" }],
              },
              output: [{ name: "id" }],
            },
            output: [{ name: "id" }],
          },
          output: [{ name: "id" }],
        },
        {},
      ),
    );
    const withRows = unwrapProviderOperationResult(await provider.execute(withCompiled, {}));

    expect(withRows).toEqual([
      {
        steps: "with:recent_orders > with-select:column:id > with-order:id",
      },
    ]);
  });
});
