import { describe, expect, it } from "vitest";

import { defineSchema, parseSql } from "../src";

const schema = defineSchema({
  tables: {
    agent_events: {
      columns: {
        event_id: "text",
      },
    },
  },
});

describe("parseSql", () => {
  it("parses single table selects", () => {
    expect(parseSql({ text: "SELECT * FROM agent_events" }, schema)).toEqual({
      source: "agent_events",
      selectAll: true,
    });
  });

  it("rejects non-select statements", () => {
    expect(() => parseSql({ text: "DELETE FROM agent_events" }, schema)).toThrow(
      "Only SELECT statements are currently supported.",
    );
  });

  it("parses SELECT statements that include CTEs", () => {
    expect(
      parseSql(
        {
          text: `
            WITH scoped AS (
              SELECT event_id FROM agent_events
            )
            SELECT event_id FROM scoped
          `,
        },
        schema,
      ),
    ).toEqual({
      source: "scoped",
      selectAll: false,
    });
  });

  it("parses set operations", () => {
    expect(
      parseSql(
        { text: "SELECT event_id FROM agent_events UNION SELECT event_id FROM agent_events" },
        schema,
      ),
    ).toEqual({
      source: "agent_events",
      selectAll: false,
    });
  });

  it("parses SELECT DISTINCT", () => {
    expect(parseSql({ text: "SELECT DISTINCT event_id FROM agent_events" }, schema)).toEqual({
      source: "agent_events",
      selectAll: false,
    });
  });
});
