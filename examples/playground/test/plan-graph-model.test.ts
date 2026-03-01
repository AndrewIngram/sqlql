import { describe, expect, it } from "vitest";
import type { QueryExecutionPlanStep } from "sqlql";

import {
  buildPlanGraphLayout,
  buildPlanGraphModel,
  collectDependencies,
} from "../src/plan-graph-model";

const STEPS: QueryExecutionPlanStep[] = [
  {
    id: "scan:users",
    kind: "scan",
    dependsOn: [],
    summary: "scan users",
    phase: "fetch",
    operation: { name: "scan" },
  },
  {
    id: "scan:workouts",
    kind: "scan",
    dependsOn: [],
    summary: "scan workouts",
    phase: "fetch",
    operation: { name: "scan" },
  },
  {
    id: "join:uw",
    kind: "join",
    dependsOn: ["scan:users", "scan:workouts"],
    summary: "join users/workouts",
    phase: "transform",
    operation: { name: "join" },
  },
  {
    id: "order",
    kind: "order",
    dependsOn: ["join:uw"],
    summary: "order results",
    phase: "output",
    operation: { name: "order" },
  },
];

describe("playground/plan-graph-model", () => {
  it("builds deterministic node positions and edge count", () => {
    const first = buildPlanGraphLayout(STEPS);
    const second = buildPlanGraphLayout(STEPS);

    expect(first.edges).toHaveLength(3);
    expect(first.steps.map((step) => step.id)).toEqual(second.steps.map((step) => step.id));

    for (const step of first.steps) {
      expect(first.positionsById.get(step.id)).toEqual(second.positionsById.get(step.id));
    }
  });

  it("collects upstream/downstream dependencies from selected node", () => {
    const deps = collectDependencies(STEPS, "join:uw");

    expect([...deps.upstream].sort()).toEqual(["scan:users", "scan:workouts"]);
    expect([...deps.downstream].sort()).toEqual(["order"]);
  });

  it("marks selected dependency path in graph model", () => {
    const layout = buildPlanGraphLayout(STEPS);
    const model = buildPlanGraphModel(layout, {}, "join:uw", null);

    const selectedNode = model.nodes.find((node) => node.id === "join:uw");
    expect(selectedNode?.data.isSelected).toBe(true);

    const highlightedNodes = model.nodes.filter((node) => node.data.isHighlighted).map((node) => node.id);
    expect(highlightedNodes.sort()).toEqual(["join:uw", "order", "scan:users", "scan:workouts"]);
  });
});
