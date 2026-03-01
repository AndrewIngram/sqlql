import dagre from "dagre";
import { MarkerType, Position, type Edge, type Node } from "@xyflow/react";
import type { QueryExecutionPlanStep, QueryStepState } from "sqlql";

const NODE_WIDTH = 320;
const NODE_HEIGHT = 170;

interface LayoutPosition {
  x: number;
  y: number;
}

export interface PlanNodeData extends Record<string, unknown> {
  step: QueryExecutionPlanStep;
  state: QueryStepState | null;
  isCurrent: boolean;
  isSelected: boolean;
  isHighlighted: boolean;
}

export interface PlanGraphModel {
  nodes: Array<Node<PlanNodeData>>;
  edges: Edge[];
}

export interface PlanGraphLayout {
  steps: QueryExecutionPlanStep[];
  positionsById: Map<string, LayoutPosition>;
  edges: Array<{
    source: string;
    target: string;
  }>;
}

export function collectDependencies(
  steps: QueryExecutionPlanStep[],
  selectedStepId: string,
): {
  upstream: Set<string>;
  downstream: Set<string>;
} {
  const upstream = new Set<string>();
  const downstream = new Set<string>();

  const byId = new Map(steps.map((step) => [step.id, step]));
  const forward = new Map<string, string[]>();

  for (const step of steps) {
    for (const dependencyId of step.dependsOn) {
      const list = forward.get(dependencyId) ?? [];
      list.push(step.id);
      forward.set(dependencyId, list);
    }
  }

  const walkUp = (stepId: string): void => {
    const step = byId.get(stepId);
    if (!step) {
      return;
    }

    for (const dependencyId of step.dependsOn) {
      if (upstream.has(dependencyId)) {
        continue;
      }

      upstream.add(dependencyId);
      walkUp(dependencyId);
    }
  };

  const walkDown = (stepId: string): void => {
    const next = forward.get(stepId) ?? [];
    for (const childId of next) {
      if (downstream.has(childId)) {
        continue;
      }

      downstream.add(childId);
      walkDown(childId);
    }
  };

  walkUp(selectedStepId);
  walkDown(selectedStepId);

  return { upstream, downstream };
}

export function buildPlanGraphLayout(steps: QueryExecutionPlanStep[]): PlanGraphLayout {
  const graph = new dagre.graphlib.Graph();
  graph.setDefaultEdgeLabel(() => ({}));
  graph.setGraph({
    rankdir: "LR",
    ranksep: 280,
    nodesep: 170,
    marginx: 12,
    marginy: 12,
  });

  const sortedSteps = [...steps].sort((left, right) => {
    if (left.dependsOn.length !== right.dependsOn.length) {
      return left.dependsOn.length - right.dependsOn.length;
    }

    return left.id.localeCompare(right.id);
  });

  for (const step of sortedSteps) {
    graph.setNode(step.id, {
      width: NODE_WIDTH,
      height: NODE_HEIGHT,
    });
  }

  const edges: Array<{ source: string; target: string }> = [];
  for (const step of sortedSteps) {
    for (const dependencyId of step.dependsOn) {
      graph.setEdge(dependencyId, step.id);
      edges.push({ source: dependencyId, target: step.id });
    }
  }

  dagre.layout(graph);

  const positionsById = new Map<string, LayoutPosition>();
  for (const step of sortedSteps) {
    const layoutNode = graph.node(step.id);
    positionsById.set(step.id, {
      x: layoutNode.x - NODE_WIDTH / 2,
      y: layoutNode.y - NODE_HEIGHT / 2,
    });
  }

  return {
    steps: sortedSteps,
    positionsById,
    edges,
  };
}

export function buildPlanGraphModel(
  layout: PlanGraphLayout,
  statesById: Record<string, QueryStepState | undefined>,
  selectedStepId: string | null,
  currentStepId: string | null,
): PlanGraphModel {
  if (layout.steps.length === 0) {
    return { nodes: [], edges: [] };
  }

  const focusStepId = selectedStepId ?? currentStepId;
  const highlighted = new Set<string>();

  if (focusStepId) {
    highlighted.add(focusStepId);
    const { upstream, downstream } = collectDependencies(layout.steps, focusStepId);
    for (const id of upstream) {
      highlighted.add(id);
    }
    for (const id of downstream) {
      highlighted.add(id);
    }
  }

  const nodes: Array<Node<PlanNodeData>> = layout.steps.map((step) => {
    const position = layout.positionsById.get(step.id);
    const isSelected = step.id === selectedStepId;
    const isCurrent = step.id === currentStepId;
    const isHighlighted = focusStepId ? highlighted.has(step.id) : false;

    return {
      id: step.id,
      type: "planStep",
      position: position ?? { x: 0, y: 0 },
      data: {
        step,
        state: statesById[step.id] ?? null,
        isCurrent,
        isSelected,
        isHighlighted,
      },
      targetPosition: Position.Left,
      sourcePosition: Position.Right,
      draggable: false,
      selectable: true,
    };
  });

  const edges: Edge[] = layout.edges.map((edge) => {
    const inFocusPath =
      focusStepId != null && highlighted.has(edge.source) && highlighted.has(edge.target);

    return {
      id: `${edge.source}->${edge.target}`,
      source: edge.source,
      target: edge.target,
      type: "smoothstep",
      pathOptions: { borderRadius: 12, offset: 26 },
      animated: false,
      markerEnd: { type: MarkerType.ArrowClosed, width: 16, height: 16 },
      style: {
        stroke: inFocusPath ? "#0284c7" : "#94a3b8",
        strokeWidth: inFocusPath ? 2.6 : 1.4,
        opacity: inFocusPath || focusStepId == null ? 1 : 0.5,
      },
    };
  });

  return { nodes, edges };
}
