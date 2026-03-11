import { buildEntitySchema } from "../../../../../test/support/schema-builder";
import type { RowsByTable } from "../../../../../test/support/query-harness";
import { registerParityCases, type ComplianceCase } from "../../../../../test/support/case-runner";

const schema = buildEntitySchema({
  items: {
    columns: {
      id: { type: "text", nullable: false },
      group_key: { type: "text", nullable: true },
      amount: { type: "integer", nullable: true },
      active: { type: "boolean", nullable: true },
    },
  },
});

const rowsByTable = {
  items: [
    { id: "item_1", group_key: null, amount: null, active: null },
    { id: "item_2", group_key: "a", amount: 10, active: true },
    { id: "item_3", group_key: "a", amount: null, active: false },
    { id: "item_4", group_key: "b", amount: 7, active: true },
    { id: "item_5", group_key: "b", amount: 3, active: false },
  ],
} satisfies RowsByTable<typeof schema>;

const cases: ComplianceCase[] = [
  {
    name: "NULL equality returns unknown (filtered out)",
    sql: `
      SELECT id
      FROM items
      WHERE group_key = NULL
      ORDER BY id ASC
    `,
    expectedRows: [],
  },
  {
    name: "IS NULL",
    sql: `
      SELECT id
      FROM items
      WHERE group_key IS NULL
      ORDER BY id ASC
    `,
    expectedRows: [{ id: "item_1" }],
  },
  {
    name: "IN list containing NULL",
    sql: `
      SELECT id
      FROM items
      WHERE group_key IN ('a', NULL)
      ORDER BY id ASC
    `,
    expectedRows: [{ id: "item_2" }, { id: "item_3" }],
  },
  {
    name: "ORDER BY NULL placement",
    sql: `
      SELECT id, group_key
      FROM items
      ORDER BY group_key ASC, id ASC
    `,
    expectedRows: [
      { id: "item_1", group_key: null },
      { id: "item_2", group_key: "a" },
      { id: "item_3", group_key: "a" },
      { id: "item_4", group_key: "b" },
      { id: "item_5", group_key: "b" },
    ],
  },
  {
    name: "GROUP BY NULL key + aggregate null handling",
    sql: `
      SELECT group_key, COUNT(*) AS n_all, COUNT(amount) AS n_amount, SUM(amount) AS sum_amount
      FROM items
      GROUP BY group_key
      ORDER BY group_key ASC
    `,
    expectedRows: [
      { group_key: null, n_all: 1, n_amount: 0, sum_amount: null },
      { group_key: "a", n_all: 2, n_amount: 1, sum_amount: 10 },
      { group_key: "b", n_all: 2, n_amount: 2, sum_amount: 10 },
    ],
  },
  {
    name: "DISTINCT includes NULL once",
    sql: `
      SELECT DISTINCT group_key
      FROM items
      ORDER BY group_key ASC
    `,
    expectedRows: [{ group_key: null }, { group_key: "a" }, { group_key: "b" }],
  },
  {
    name: "BETWEEN excludes NULL values",
    sql: `
      SELECT id
      FROM items
      WHERE amount BETWEEN 4 AND 10
      ORDER BY id ASC
    `,
    expectedRows: [{ id: "item_2" }, { id: "item_4" }],
  },
  {
    name: "boolean literal comparisons",
    sql: `
      SELECT id
      FROM items
      WHERE active = true OR active = false
      ORDER BY id ASC
    `,
    expectedRows: [{ id: "item_2" }, { id: "item_3" }, { id: "item_4" }, { id: "item_5" }],
  },
];

registerParityCases(
  "compliance/null-semantics-parity",
  {
    schema,
    rowsByTable,
  },
  cases,
);
