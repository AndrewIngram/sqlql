import { Result } from "better-result";
import { describe, expect, it } from "vitest";

import {
  expandRelViewsResult,
  lowerSqlToRel,
  normalizePhysicalPlanForSnapshot,
  normalizeRelForSnapshot,
  planPhysicalQuery,
} from "@tupl/planner";
import { buildEntitySchema } from "@tupl/test-support/schema";
import { finalizeProviders } from "@tupl/test-support/runtime";

describe("query/translation-snapshots", () => {
  it("captures initial and rewritten trees for SELECT without FROM", () => {
    const schema = buildEntitySchema({});

    const lowered = lowerSqlToRel(
      `
        SELECT 1 AS answer, 2 + 3 AS sum_value
      `,
      schema,
    );
    const rewritten = expandRelViewsResult(lowered.rel, schema, {});
    if (Result.isError(rewritten)) {
      throw rewritten.error;
    }

    expect(normalizeRelForSnapshot(lowered.rel)).toMatchInlineSnapshot(`
      {
        "columns": [
          {
            "expr": {
              "kind": "literal",
              "value": 1,
            },
            "kind": "expr",
            "output": "answer",
          },
          {
            "expr": {
              "args": [
                {
                  "kind": "literal",
                  "value": 2,
                },
                {
                  "kind": "literal",
                  "value": 3,
                },
              ],
              "kind": "function",
              "name": "add",
            },
            "kind": "expr",
            "output": "sum_value",
          },
        ],
        "convention": "local",
        "id": "project_1",
        "input": {
          "convention": "local",
          "id": "values_2",
          "kind": "values",
          "output": [],
          "rows": [
            [],
          ],
        },
        "kind": "project",
        "output": [
          {
            "name": "answer",
          },
          {
            "name": "sum_value",
          },
        ],
      }
    `);

    expect(normalizeRelForSnapshot(rewritten.value)).toMatchInlineSnapshot(`
      {
        "columns": [
          {
            "expr": {
              "kind": "literal",
              "value": 1,
            },
            "kind": "expr",
            "output": "answer",
          },
          {
            "expr": {
              "args": [
                {
                  "kind": "literal",
                  "value": 2,
                },
                {
                  "kind": "literal",
                  "value": 3,
                },
              ],
              "kind": "function",
              "name": "add",
            },
            "kind": "expr",
            "output": "sum_value",
          },
        ],
        "convention": "local",
        "id": "project_1",
        "input": {
          "convention": "local",
          "id": "values_2",
          "kind": "values",
          "output": [],
          "rows": [
            [],
          ],
        },
        "kind": "project",
        "output": [
          {
            "name": "answer",
          },
          {
            "name": "sum_value",
          },
        ],
      }
    `);
  });

  it("captures full initial and rewritten trees for derived tables", () => {
    const schema = buildEntitySchema({
      orders: {
        provider: "warehouse",
        columns: {
          id: "text",
          org_id: "text",
        },
      },
    });

    const lowered = lowerSqlToRel(
      `
        SELECT scoped.id
        FROM (
          SELECT id
          FROM orders
          WHERE org_id = 'org_1'
        ) scoped
        ORDER BY scoped.id ASC
      `,
      schema,
    );
    const rewritten = expandRelViewsResult(lowered.rel, schema, {});
    if (Result.isError(rewritten)) {
      throw rewritten.error;
    }

    expect(normalizeRelForSnapshot(lowered.rel)).toMatchInlineSnapshot(`
      {
        "body": {
          "columns": [
            {
              "kind": "column",
              "output": "id",
              "source": {
                "alias": "scoped",
                "column": "id",
              },
            },
          ],
          "convention": "local",
          "id": "project_4",
          "input": {
            "convention": "local",
            "id": "sort_5",
            "input": {
              "alias": "scoped",
              "convention": "local",
              "id": "scan_6",
              "kind": "scan",
              "output": [
                {
                  "name": "scoped.id",
                },
              ],
              "select": [
                "id",
              ],
              "table": "__tupl_derived_1",
            },
            "kind": "sort",
            "orderBy": [
              {
                "direction": "asc",
                "source": {
                  "alias": "scoped",
                  "column": "id",
                },
              },
            ],
            "output": [
              {
                "name": "scoped.id",
              },
            ],
          },
          "kind": "project",
          "output": [
            {
              "name": "id",
            },
          ],
        },
        "convention": "local",
        "ctes": [
          {
            "name": "__tupl_derived_1",
            "query": {
              "columns": [
                {
                  "kind": "column",
                  "output": "id",
                  "source": {
                    "alias": "orders",
                    "column": "id",
                  },
                },
              ],
              "convention": "local",
              "id": "project_2",
              "input": {
                "alias": "orders",
                "convention": "local",
                "id": "scan_3",
                "kind": "scan",
                "output": [
                  {
                    "name": "orders.id",
                  },
                  {
                    "name": "orders.org_id",
                  },
                ],
                "select": [
                  "id",
                  "org_id",
                ],
                "table": "orders",
                "where": [
                  {
                    "column": "org_id",
                    "op": "eq",
                    "value": "org_1",
                  },
                ],
              },
              "kind": "project",
              "output": [
                {
                  "name": "id",
                },
              ],
            },
          },
        ],
        "id": "with_1",
        "kind": "with",
        "output": [
          {
            "name": "id",
          },
        ],
      }
    `);
    expect(normalizeRelForSnapshot(rewritten.value)).toMatchInlineSnapshot(`
      {
        "body": {
          "columns": [
            {
              "kind": "column",
              "output": "id",
              "source": {
                "alias": "scoped",
                "column": "id",
              },
            },
          ],
          "convention": "local",
          "id": "project_4",
          "input": {
            "convention": "local",
            "id": "sort_5",
            "input": {
              "alias": "scoped",
              "convention": "local",
              "id": "scan_6",
              "kind": "scan",
              "output": [
                {
                  "name": "scoped.id",
                },
              ],
              "select": [
                "id",
              ],
              "table": "__tupl_derived_1",
            },
            "kind": "sort",
            "orderBy": [
              {
                "direction": "asc",
                "source": {
                  "alias": "scoped",
                  "column": "id",
                },
              },
            ],
            "output": [
              {
                "name": "scoped.id",
              },
            ],
          },
          "kind": "project",
          "output": [
            {
              "name": "id",
            },
          ],
        },
        "convention": "local",
        "ctes": [
          {
            "name": "__tupl_derived_1",
            "query": {
              "columns": [
                {
                  "kind": "column",
                  "output": "id",
                  "source": {
                    "alias": "orders",
                    "column": "id",
                  },
                },
              ],
              "convention": "local",
              "id": "project_2",
              "input": {
                "alias": "orders",
                "convention": "local",
                "id": "scan_3",
                "kind": "scan",
                "output": [
                  {
                    "name": "orders.id",
                  },
                  {
                    "name": "orders.org_id",
                  },
                ],
                "select": [
                  "id",
                  "org_id",
                ],
                "table": "orders",
                "where": [
                  {
                    "column": "org_id",
                    "op": "eq",
                    "value": "org_1",
                  },
                ],
              },
              "kind": "project",
              "output": [
                {
                  "name": "id",
                },
              ],
            },
          },
        ],
        "id": "with_1",
        "kind": "with",
        "output": [
          {
            "name": "id",
          },
        ],
      }
    `);
  });

  it("captures recursive cte trees without unstable rel ids", () => {
    const schema = buildEntitySchema({
      edges: {
        provider: "warehouse",
        columns: {
          source_id: "integer",
          target_id: "integer",
        },
      },
    });

    const lowered = lowerSqlToRel(
      `
        WITH RECURSIVE reachable AS (
          SELECT source_id AS node_id
          FROM edges
          WHERE source_id = 1
          UNION ALL
          SELECT e.target_id AS node_id
          FROM reachable r
          JOIN edges e ON e.source_id = r.node_id
        )
        SELECT node_id
        FROM reachable
      `,
      schema,
    );

    expect(normalizeRelForSnapshot(lowered.rel)).toMatchInlineSnapshot(`
      {
        "body": {
          "columns": [
            {
              "kind": "column",
              "output": "node_id",
              "source": {
                "alias": "reachable",
                "column": "node_id",
              },
            },
          ],
          "convention": "local",
          "id": "project_9",
          "input": {
            "alias": "reachable",
            "convention": "local",
            "id": "scan_10",
            "kind": "scan",
            "output": [
              {
                "name": "reachable.node_id",
              },
            ],
            "select": [
              "node_id",
            ],
            "table": "reachable",
          },
          "kind": "project",
          "output": [
            {
              "name": "node_id",
            },
          ],
        },
        "convention": "local",
        "ctes": [
          {
            "name": "reachable",
            "query": {
              "convention": "logical",
              "cteName": "reachable",
              "id": "repeat_union_2",
              "iterative": {
                "columns": [
                  {
                    "kind": "column",
                    "output": "node_id",
                    "source": {
                      "alias": "e",
                      "column": "target_id",
                    },
                  },
                ],
                "convention": "local",
                "id": "project_5",
                "input": {
                  "convention": "local",
                  "id": "join_6",
                  "joinType": "inner",
                  "kind": "join",
                  "left": {
                    "alias": "r",
                    "convention": "local",
                    "id": "scan_7",
                    "kind": "scan",
                    "output": [
                      {
                        "name": "r.node_id",
                      },
                    ],
                    "select": [
                      "node_id",
                    ],
                    "table": "reachable",
                  },
                  "leftKey": {
                    "alias": "r",
                    "column": "node_id",
                  },
                  "output": [
                    {
                      "name": "r.node_id",
                    },
                    {
                      "name": "e.target_id",
                    },
                    {
                      "name": "e.source_id",
                    },
                  ],
                  "right": {
                    "alias": "e",
                    "convention": "local",
                    "id": "scan_8",
                    "kind": "scan",
                    "output": [
                      {
                        "name": "e.target_id",
                      },
                      {
                        "name": "e.source_id",
                      },
                    ],
                    "select": [
                      "target_id",
                      "source_id",
                    ],
                    "table": "edges",
                  },
                  "rightKey": {
                    "alias": "e",
                    "column": "source_id",
                  },
                },
                "kind": "project",
                "output": [
                  {
                    "name": "node_id",
                  },
                ],
              },
              "kind": "repeat_union",
              "mode": "union_all",
              "output": [
                {
                  "name": "node_id",
                },
              ],
              "seed": {
                "columns": [
                  {
                    "kind": "column",
                    "output": "node_id",
                    "source": {
                      "alias": "edges",
                      "column": "source_id",
                    },
                  },
                ],
                "convention": "local",
                "id": "project_3",
                "input": {
                  "alias": "edges",
                  "convention": "local",
                  "id": "scan_4",
                  "kind": "scan",
                  "output": [
                    {
                      "name": "edges.source_id",
                    },
                  ],
                  "select": [
                    "source_id",
                  ],
                  "table": "edges",
                  "where": [
                    {
                      "column": "source_id",
                      "op": "eq",
                      "value": 1,
                    },
                  ],
                },
                "kind": "project",
                "output": [
                  {
                    "name": "node_id",
                  },
                ],
              },
            },
          },
        ],
        "id": "with_1",
        "kind": "with",
        "output": [
          {
            "name": "node_id",
          },
        ],
      }
    `);
  });

  it("captures correlated exists after decorrelation into a semi join tree", () => {
    const schema = buildEntitySchema({
      orders: {
        provider: "warehouse",
        columns: {
          id: "text",
          user_id: "text",
        },
      },
      users: {
        provider: "warehouse",
        columns: {
          id: "text",
          team_id: "text",
        },
      },
    });

    const lowered = lowerSqlToRel(
      `
        SELECT o.id
        FROM orders o
        WHERE EXISTS (
          SELECT 1
          FROM users u
          WHERE u.id = o.user_id
            AND u.team_id = 'team_smb'
        )
      `,
      schema,
    );

    expect(normalizeRelForSnapshot(lowered.rel)).toMatchInlineSnapshot(`
      {
        "columns": [
          {
            "kind": "column",
            "output": "id",
            "source": {
              "alias": "o",
              "column": "id",
            },
          },
        ],
        "convention": "local",
        "id": "project_1",
        "input": {
          "convention": "local",
          "id": "join_2",
          "joinType": "semi",
          "kind": "join",
          "left": {
            "alias": "o",
            "convention": "local",
            "id": "scan_3",
            "kind": "scan",
            "output": [
              {
                "name": "o.id",
              },
              {
                "name": "o.user_id",
              },
            ],
            "select": [
              "id",
              "user_id",
            ],
            "table": "orders",
          },
          "leftKey": {
            "alias": "o",
            "column": "user_id",
          },
          "output": [
            {
              "name": "o.id",
            },
            {
              "name": "o.user_id",
            },
          ],
          "right": {
            "columns": [
              {
                "expr": {
                  "kind": "literal",
                  "value": 1,
                },
                "kind": "expr",
                "output": "expr",
              },
              {
                "kind": "column",
                "output": "u.id",
                "source": {
                  "alias": "u",
                  "column": "id",
                },
              },
            ],
            "convention": "local",
            "id": "project_4",
            "input": {
              "alias": "u",
              "convention": "local",
              "id": "scan_5",
              "kind": "scan",
              "output": [
                {
                  "name": "u.team_id",
                },
              ],
              "select": [
                "team_id",
              ],
              "table": "users",
              "where": [
                {
                  "column": "team_id",
                  "op": "eq",
                  "value": "team_smb",
                },
              ],
            },
            "kind": "project",
            "output": [
              {
                "name": "expr",
              },
              {
                "name": "u.id",
              },
            ],
          },
          "rightKey": {
            "alias": "u",
            "column": "id",
          },
        },
        "kind": "project",
        "output": [
          {
            "name": "id",
          },
        ],
      }
    `);
  });

  it("captures mixed physical plans for cross-provider joins", async () => {
    const schema = buildEntitySchema({
      orders: {
        provider: "orders",
        columns: {
          id: "text",
          user_id: "text",
        },
      },
      users: {
        provider: "users",
        columns: {
          id: "text",
          email: "text",
        },
      },
    });
    const providers = finalizeProviders({
      orders: {
        canExecute() {
          return true;
        },
        async compile(fragment) {
          return {
            ok: true as const,
            value: { provider: "orders", kind: fragment.kind, payload: fragment },
          };
        },
        async execute() {
          return { ok: true as const, value: [] };
        },
      },
      users: {
        canExecute() {
          return true;
        },
        async compile(fragment) {
          return {
            ok: true as const,
            value: { provider: "users", kind: fragment.kind, payload: fragment },
          };
        },
        async execute() {
          return { ok: true as const, value: [] };
        },
      },
    });

    const lowered = lowerSqlToRel(
      `
        SELECT o.id, u.email
        FROM orders o
        JOIN users u ON o.user_id = u.id
      `,
      schema,
    );

    const physical = await planPhysicalQuery(
      lowered.rel,
      schema,
      providers,
      {},
      `
        SELECT o.id, u.email
        FROM orders o
        JOIN users u ON o.user_id = u.id
      `,
    );

    expect(normalizePhysicalPlanForSnapshot(physical)).toMatchInlineSnapshot(`
      {
        "rel": {
          "columns": [
            {
              "kind": "column",
              "output": "id",
              "source": {
                "alias": "o",
                "column": "id",
              },
            },
            {
              "kind": "column",
              "output": "email",
              "source": {
                "alias": "u",
                "column": "email",
              },
            },
          ],
          "convention": "local",
          "id": "project_1",
          "input": {
            "convention": "local",
            "id": "join_2",
            "joinType": "inner",
            "kind": "join",
            "left": {
              "alias": "o",
              "convention": "provider:orders",
              "id": "scan_3",
              "kind": "scan",
              "output": [
                {
                  "name": "o.id",
                },
                {
                  "name": "o.user_id",
                },
              ],
              "select": [
                "id",
                "user_id",
              ],
              "table": "orders",
            },
            "leftKey": {
              "alias": "o",
              "column": "user_id",
            },
            "output": [
              {
                "name": "o.id",
              },
              {
                "name": "o.user_id",
              },
              {
                "name": "u.email",
              },
              {
                "name": "u.id",
              },
            ],
            "right": {
              "alias": "u",
              "convention": "provider:users",
              "id": "scan_4",
              "kind": "scan",
              "output": [
                {
                  "name": "u.email",
                },
                {
                  "name": "u.id",
                },
              ],
              "select": [
                "email",
                "id",
              ],
              "table": "users",
            },
            "rightKey": {
              "alias": "u",
              "column": "id",
            },
          },
          "kind": "project",
          "output": [
            {
              "name": "id",
            },
            {
              "name": "email",
            },
          ],
        },
        "rootStepId": "step_4",
        "steps": [
          {
            "dependsOn": [],
            "fragment": {
              "kind": "scan",
              "provider": "orders",
              "request": {
                "alias": "o",
                "select": [
                  "id",
                  "user_id",
                ],
                "table": "orders",
              },
              "table": "orders",
            },
            "id": "step_1",
            "kind": "remote_fragment",
            "provider": "orders",
            "summary": "Execute provider fragment (orders)",
          },
          {
            "dependsOn": [],
            "fragment": {
              "kind": "scan",
              "provider": "users",
              "request": {
                "alias": "u",
                "select": [
                  "email",
                  "id",
                ],
                "table": "users",
              },
              "table": "users",
            },
            "id": "step_2",
            "kind": "remote_fragment",
            "provider": "users",
            "summary": "Execute provider fragment (users)",
          },
          {
            "dependsOn": [
              "step_1",
              "step_2",
            ],
            "id": "step_3",
            "kind": "local_hash_join",
            "summary": "Local inner join execution",
          },
          {
            "dependsOn": [
              "step_3",
            ],
            "id": "step_4",
            "kind": "local_project",
            "summary": "Local project execution",
          },
        ],
      }
    `);
  });
});
