import { describe, expect, it } from "vite-plus/test";
import type { QueryExecutionPlanScope, QueryExecutionPlanStep } from "@tupl/runtime/session";

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

const SCOPES: QueryExecutionPlanScope[] = [
  {
    id: "scope:root",
    kind: "root",
    label: "Root",
  },
  {
    id: "scope:cte",
    kind: "cte",
    label: "CTE recent_workouts",
    parentId: "scope:root",
  },
  {
    id: "scope:subquery",
    kind: "subquery",
    label: "Subquery WHERE #1",
    parentId: "scope:cte",
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
    const model = buildPlanGraphModel(layout, [], {}, "join:uw", null);

    const selectedNode = model.nodes.find(
      (node) => node.id === "join:uw" && node.type === "planStep",
    );
    expect(
      selectedNode && selectedNode.type === "planStep" ? selectedNode.data.isSelected : false,
    ).toBe(true);

    const highlightedNodes = model.nodes
      .filter((node) => node.type === "planStep" && node.data.isHighlighted)
      .map((node) => node.id);
    expect(highlightedNodes.sort()).toEqual(["join:uw", "order", "scan:users", "scan:workouts"]);
  });

  it("adds nested scope parent nodes and assigns step parentId", () => {
    const [scanUsers, scanWorkouts, joinUsersWorkouts, orderResults] = STEPS;
    if (!scanUsers || !scanWorkouts || !joinUsersWorkouts || !orderResults) {
      throw new Error("Missing expected test fixture steps.");
    }
    const scopedSteps: QueryExecutionPlanStep[] = [
      {
        ...scanUsers,
        scopeId: "scope:cte",
      },
      {
        ...scanWorkouts,
        scopeId: "scope:subquery",
      },
      {
        ...joinUsersWorkouts,
        scopeId: "scope:root",
      },
      {
        ...orderResults,
        scopeId: "scope:root",
      },
    ];
    const layout = buildPlanGraphLayout(scopedSteps);
    const model = buildPlanGraphModel(layout, SCOPES, {}, null, null);

    const cteScopeNode = model.nodes.find((node) => node.id === "scope:cte");
    expect(cteScopeNode).toBeDefined();
    expect(cteScopeNode?.type).toBe("planScope");

    const subqueryScopeNode = model.nodes.find((node) => node.id === "scope:subquery");
    expect(subqueryScopeNode).toBeDefined();
    expect(subqueryScopeNode?.type).toBe("planScope");
    expect(subqueryScopeNode?.parentId).toBe("scope:cte");

    const usersNode = model.nodes.find((node) => node.id === "scan:users");
    expect(usersNode?.parentId).toBe("scope:cte");

    const workoutsNode = model.nodes.find((node) => node.id === "scan:workouts");
    expect(workoutsNode?.parentId).toBe("scope:subquery");

    const joinNode = model.nodes.find((node) => node.id === "join:uw");
    expect(joinNode?.parentId).toBeUndefined();
  });

  it("repositions root sibling steps to avoid overlapping top-level scope containers", () => {
    const scopedStep: QueryExecutionPlanStep = {
      id: "cte:scan",
      kind: "scan",
      dependsOn: [],
      summary: "scan CTE rows",
      phase: "fetch",
      operation: { name: "scan" },
      scopeId: "scope:cte",
    };

    const siblingStep: QueryExecutionPlanStep = {
      id: "root:project",
      kind: "projection",
      dependsOn: ["cte:scan"],
      summary: "project root rows",
      phase: "output",
      operation: { name: "project" },
      scopeId: "scope:root",
    };

    const layout = {
      steps: [scopedStep, siblingStep],
      positionsById: new Map([
        ["cte:scan", { x: 0, y: 0 }],
        ["root:project", { x: 12, y: 8 }],
      ]),
      edges: [{ source: "cte:scan", target: "root:project" }],
    };

    const model = buildPlanGraphModel(layout, SCOPES, {}, null, null);

    const scopeNode = model.nodes.find((node) => node.id === "scope:cte");
    const rootNode = model.nodes.find((node) => node.id === "root:project");
    if (!scopeNode || !rootNode) {
      throw new Error("Expected scope and root nodes.");
    }

    const scopeX = scopeNode.position.x;
    const scopeY = scopeNode.position.y;
    const scopeWidth = Number(scopeNode.style?.width ?? 0);
    const scopeHeight = Number(scopeNode.style?.height ?? 0);
    expect(rootNode.parentId).toBeUndefined();

    const rootRect = {
      x: rootNode.position.x,
      y: rootNode.position.y,
      width: 320,
      height: 170,
    };
    const scopeRect = {
      x: scopeX,
      y: scopeY,
      width: scopeWidth,
      height: scopeHeight,
    };
    const overlap =
      rootRect.x < scopeRect.x + scopeRect.width &&
      rootRect.x + rootRect.width > scopeRect.x &&
      rootRect.y < scopeRect.y + scopeRect.height &&
      rootRect.y + rootRect.height > scopeRect.y;
    expect(overlap).toBe(false);
  });

  it("packs root siblings so they do not overlap each other after scope avoidance", () => {
    const scopedStep: QueryExecutionPlanStep = {
      id: "cte:scan",
      kind: "scan",
      dependsOn: [],
      summary: "scan CTE rows",
      phase: "fetch",
      operation: { name: "scan" },
      scopeId: "scope:cte",
    };

    const rootA: QueryExecutionPlanStep = {
      id: "window_9",
      kind: "window",
      dependsOn: ["cte:scan"],
      summary: "window",
      phase: "transform",
      operation: { name: "window" },
      scopeId: "scope:root",
    };

    const rootB: QueryExecutionPlanStep = {
      id: "projection_12",
      kind: "projection",
      dependsOn: ["window_9"],
      summary: "projection",
      phase: "output",
      operation: { name: "project" },
      scopeId: "scope:root",
    };

    const layout = {
      steps: [scopedStep, rootA, rootB],
      positionsById: new Map([
        ["cte:scan", { x: 0, y: 0 }],
        ["window_9", { x: 20, y: 240 }],
        ["projection_12", { x: 40, y: 280 }],
      ]),
      edges: [
        { source: "cte:scan", target: "window_9" },
        { source: "window_9", target: "projection_12" },
      ],
    };

    const model = buildPlanGraphModel(layout, SCOPES, {}, null, null);
    const nodeA = model.nodes.find((node) => node.id === "window_9");
    const nodeB = model.nodes.find((node) => node.id === "projection_12");
    if (!nodeA || !nodeB) {
      throw new Error("Expected root sibling nodes.");
    }

    const aRect = {
      x: nodeA.position.x,
      y: nodeA.position.y,
      width: 320,
      height: 170,
    };
    const bRect = {
      x: nodeB.position.x,
      y: nodeB.position.y,
      width: 320,
      height: 170,
    };

    const overlap =
      aRect.x < bRect.x + bRect.width &&
      aRect.x + aRect.width > bRect.x &&
      aRect.y < bRect.y + bRect.height &&
      aRect.y + aRect.height > bRect.y;
    expect(overlap).toBe(false);
  });
});
