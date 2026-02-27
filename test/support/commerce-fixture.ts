import { defineSchema } from "../../src";
import type { RowsByTable } from "./query-harness";

export const commerceSchema = defineSchema({
  tables: {
    orders: {
      columns: {
        id: { type: "text", nullable: false },
        org_id: { type: "text", nullable: false },
        user_id: { type: "text", nullable: false },
        status: { type: "text", nullable: false },
        total_cents: { type: "integer", nullable: false },
        created_at: { type: "timestamp", nullable: false },
      },
    },
    users: {
      columns: {
        id: { type: "text", nullable: false },
        team_id: { type: "text", nullable: true },
        email: { type: "text", nullable: true },
      },
    },
    teams: {
      columns: {
        id: { type: "text", nullable: false },
        name: { type: "text", nullable: false },
        tier: { type: "text", nullable: false },
      },
    },
  },
});

export const commerceRows = {
  orders: [
    {
      id: "ord_1",
      org_id: "org_1",
      user_id: "usr_1",
      status: "paid",
      total_cents: 1200,
      created_at: "2026-02-01",
    },
    {
      id: "ord_2",
      org_id: "org_1",
      user_id: "usr_1",
      status: "paid",
      total_cents: 1800,
      created_at: "2026-02-03",
    },
    {
      id: "ord_3",
      org_id: "org_1",
      user_id: "usr_2",
      status: "paid",
      total_cents: 2400,
      created_at: "2026-02-04",
    },
    {
      id: "ord_4",
      org_id: "org_2",
      user_id: "usr_3",
      status: "paid",
      total_cents: 9900,
      created_at: "2026-02-05",
    },
  ],
  users: [
    { id: "usr_1", team_id: "team_enterprise", email: "alice@example.com" },
    { id: "usr_2", team_id: "team_smb", email: "bob@example.com" },
    { id: "usr_3", team_id: "team_enterprise", email: "charlie@example.com" },
  ],
  teams: [
    { id: "team_enterprise", name: "Enterprise", tier: "enterprise" },
    { id: "team_smb", name: "SMB", tier: "smb" },
  ],
} satisfies RowsByTable<typeof commerceSchema>;
