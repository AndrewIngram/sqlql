import dagre from "dagre";
import { MarkerType, Position, type Edge, type Node } from "@xyflow/react";
import type {
  QueryExecutionPlanScope,
  QueryExecutionPlanStep,
  QueryStepState,
} from "../../../src/index";

const NODE_WIDTH = 320;
const NODE_HEIGHT = 170;

const SCOPE_PADDING_X = 28;
const SCOPE_PADDING_TOP = 34;
const SCOPE_PADDING_BOTTOM = 18;
const MIN_SCOPE_WIDTH = NODE_WIDTH + 40;
const MIN_SCOPE_HEIGHT = NODE_HEIGHT + SCOPE_PADDING_TOP + SCOPE_PADDING_BOTTOM;
const ROOT_STEP_CLEARANCE_X = 36;
const ROOT_STEP_CLEARANCE_Y = 32;

interface LayoutPosition {
  x: number;
  y: number;
}

interface Rect {
  x: number;
  y: number;
  width: number;
  height: number;
}

function rectOverlapsWithClearance(
  candidate: Rect,
  blocker: Rect,
  clearanceX: number,
  clearanceY: number,
): boolean {
  return (
    candidate.x < blocker.x + blocker.width + clearanceX &&
    candidate.x + candidate.width > blocker.x - clearanceX &&
    candidate.y < blocker.y + blocker.height + clearanceY &&
    candidate.y + candidate.height > blocker.y - clearanceY
  );
}

export interface PlanNodeData extends Record<string, unknown> {
  step: QueryExecutionPlanStep;
  state: QueryStepState | null;
  isCurrent: boolean;
  isSelected: boolean;
  isHighlighted: boolean;
}

export interface PlanScopeNodeData extends Record<string, unknown> {
  scope: QueryExecutionPlanScope;
}

export type PlanGraphNodeData = PlanNodeData | PlanScopeNodeData;

export interface PlanGraphModel {
  nodes: Array<Node<PlanGraphNodeData>>;
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

function buildScopeBounds(
  layout: PlanGraphLayout,
  scopes: QueryExecutionPlanScope[],
): {
  boundsById: Map<string, Rect>;
  visibleParentById: Map<string, string | undefined>;
} {
  const visibleScopes = scopes.filter((scope) => scope.kind !== "root");
  const visibleScopeIds = new Set(visibleScopes.map((scope) => scope.id));
  const scopeById = new Map(scopes.map((scope) => [scope.id, scope]));

  const resolveVisibleScopeId = (scopeId: string | undefined): string | undefined => {
    let current = scopeId;
    while (current) {
      if (visibleScopeIds.has(current)) {
        return current;
      }
      current = scopeById.get(current)?.parentId;
    }
    return undefined;
  };

  const stepRectsById = new Map<string, Rect>();
  for (const step of layout.steps) {
    const position = layout.positionsById.get(step.id) ?? { x: 0, y: 0 };
    stepRectsById.set(step.id, {
      x: position.x,
      y: position.y,
      width: NODE_WIDTH,
      height: NODE_HEIGHT,
    });
  }

  const directStepIdsByScope = new Map<string, string[]>();
  for (const step of layout.steps) {
    const owner = resolveVisibleScopeId(step.scopeId);
    if (!owner) {
      continue;
    }
    const current = directStepIdsByScope.get(owner) ?? [];
    current.push(step.id);
    directStepIdsByScope.set(owner, current);
  }

  const childScopeIdsByParent = new Map<string, string[]>();
  const visibleParentById = new Map<string, string | undefined>();
  for (const scope of visibleScopes) {
    const visibleParent = resolveVisibleScopeId(scope.parentId);
    visibleParentById.set(scope.id, visibleParent);
    if (!visibleParent) {
      continue;
    }
    const current = childScopeIdsByParent.get(visibleParent) ?? [];
    current.push(scope.id);
    childScopeIdsByParent.set(visibleParent, current);
  }

  const boundsById = new Map<string, Rect>();
  const computing = new Set<string>();

  const compute = (scopeId: string): Rect | undefined => {
    const cached = boundsById.get(scopeId);
    if (cached) {
      return cached;
    }
    if (computing.has(scopeId)) {
      return undefined;
    }

    computing.add(scopeId);

    const childRects: Rect[] = [];
    for (const childScopeId of childScopeIdsByParent.get(scopeId) ?? []) {
      const childRect = compute(childScopeId);
      if (childRect) {
        childRects.push(childRect);
      }
    }

    const directStepRects = (directStepIdsByScope.get(scopeId) ?? [])
      .map((stepId) => stepRectsById.get(stepId))
      .filter((rect): rect is Rect => rect != null);

    const contentRects = [...directStepRects, ...childRects];
    if (contentRects.length === 0) {
      computing.delete(scopeId);
      return undefined;
    }

    const minX = Math.min(...contentRects.map((rect) => rect.x));
    const maxX = Math.max(...contentRects.map((rect) => rect.x + rect.width));
    const minY = Math.min(...contentRects.map((rect) => rect.y));
    const maxY = Math.max(...contentRects.map((rect) => rect.y + rect.height));

    const rect: Rect = {
      x: minX - SCOPE_PADDING_X,
      y: minY - SCOPE_PADDING_TOP,
      width: Math.max(MIN_SCOPE_WIDTH, maxX - minX + SCOPE_PADDING_X * 2),
      height: Math.max(MIN_SCOPE_HEIGHT, maxY - minY + SCOPE_PADDING_TOP + SCOPE_PADDING_BOTTOM),
    };
    boundsById.set(scopeId, rect);

    computing.delete(scopeId);
    return rect;
  };

  for (const scope of visibleScopes) {
    compute(scope.id);
  }

  return {
    boundsById,
    visibleParentById,
  };
}

export function buildPlanGraphModel(
  layout: PlanGraphLayout,
  scopes: QueryExecutionPlanScope[] | undefined,
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

  const scopeList = scopes ?? [];
  const { boundsById: scopeBoundsById, visibleParentById } = buildScopeBounds(layout, scopeList);
  const scopeById = new Map(scopeList.map((scope) => [scope.id, scope]));
  const visibleScopeIds = new Set(scopeBoundsById.keys());

  const resolveVisibleScopeId = (scopeId: string | undefined): string | undefined => {
    let current = scopeId;
    while (current) {
      if (visibleScopeIds.has(current)) {
        return current;
      }
      current = scopeById.get(current)?.parentId;
    }
    return undefined;
  };

  const parentScopeByStepId = new Map<string, string | undefined>();
  for (const step of layout.steps) {
    parentScopeByStepId.set(step.id, resolveVisibleScopeId(step.scopeId));
  }

  const topLevelScopeRects = [...scopeBoundsById.entries()]
    .filter(([scopeId]) => !visibleParentById.get(scopeId))
    .map(([, rect]) => rect)
    .sort((left, right) => left.x - right.x);

  const adjustedPositionsById = new Map(layout.positionsById);
  const rootSteps = layout.steps
    .filter((step) => !parentScopeByStepId.get(step.id))
    .sort((left, right) => {
      const leftPos = layout.positionsById.get(left.id) ?? { x: 0, y: 0 };
      const rightPos = layout.positionsById.get(right.id) ?? { x: 0, y: 0 };
      if (leftPos.x !== rightPos.x) {
        return leftPos.x - rightPos.x;
      }
      if (leftPos.y !== rightPos.y) {
        return leftPos.y - rightPos.y;
      }
      return left.id.localeCompare(right.id);
    });

  const blockedRects: Rect[] = [...topLevelScopeRects];
  for (const step of rootSteps) {
    const position = adjustedPositionsById.get(step.id) ?? { x: 0, y: 0 };
    const rect: Rect = {
      x: position.x,
      y: position.y,
      width: NODE_WIDTH,
      height: NODE_HEIGHT,
    };

    let guard = 0;
    while (guard < 120) {
      const scopeBlocker = topLevelScopeRects.find((blocked) =>
        rectOverlapsWithClearance(
          rect,
          blocked,
          ROOT_STEP_CLEARANCE_X,
          ROOT_STEP_CLEARANCE_Y,
        ));
      if (scopeBlocker) {
        rect.y = scopeBlocker.y + scopeBlocker.height + ROOT_STEP_CLEARANCE_Y;
        guard += 1;
        continue;
      }

      const siblingBlocker = blockedRects.find((blocked) =>
        rectOverlapsWithClearance(
          rect,
          blocked,
          ROOT_STEP_CLEARANCE_X,
          ROOT_STEP_CLEARANCE_Y,
        ));
      if (!siblingBlocker) {
        break;
      }

      rect.x = siblingBlocker.x + siblingBlocker.width + ROOT_STEP_CLEARANCE_X;
      guard += 1;
    }

    adjustedPositionsById.set(step.id, { x: rect.x, y: rect.y });
    blockedRects.push({ ...rect });
  }

  const depthCache = new Map<string, number>();
  const depthOf = (scopeId: string): number => {
    const cached = depthCache.get(scopeId);
    if (cached != null) {
      return cached;
    }

    const parentId = visibleParentById.get(scopeId);
    const depth = parentId ? depthOf(parentId) + 1 : 0;
    depthCache.set(scopeId, depth);
    return depth;
  };

  const scopeNodes: Array<Node<PlanGraphNodeData>> = [];
  for (const scopeId of [...scopeBoundsById.keys()].sort((left, right) => {
    const depthDiff = depthOf(left) - depthOf(right);
    if (depthDiff !== 0) {
      return depthDiff;
    }
    return left.localeCompare(right);
  })) {
    const scope = scopeById.get(scopeId);
    const rect = scopeBoundsById.get(scopeId);
    if (!scope || !rect) {
      continue;
    }

    const parentId = visibleParentById.get(scopeId);
    const parentRect = parentId ? scopeBoundsById.get(parentId) : undefined;
    const position = parentRect
      ? {
          x: rect.x - parentRect.x,
          y: rect.y - parentRect.y,
        }
      : {
          x: rect.x,
          y: rect.y,
        };

    const node: Node<PlanGraphNodeData> = {
      id: scopeId,
      type: "planScope",
      position,
      data: {
        scope,
      },
      draggable: false,
      selectable: false,
      focusable: false,
      connectable: false,
      style: {
        width: rect.width,
        height: rect.height,
      },
      ...(parentId ? { parentId, extent: "parent" as const } : {}),
    };
    scopeNodes.push(node);
  }

  const nodes: Array<Node<PlanGraphNodeData>> = layout.steps.map((step) => {
    const absolutePosition = adjustedPositionsById.get(step.id) ?? { x: 0, y: 0 };
    const parentScopeId = parentScopeByStepId.get(step.id);
    const parentRect = parentScopeId ? scopeBoundsById.get(parentScopeId) : undefined;
    const position = parentRect
      ? {
          x: absolutePosition.x - parentRect.x,
          y: absolutePosition.y - parentRect.y,
        }
      : absolutePosition;

    const isSelected = step.id === selectedStepId;
    const isCurrent = step.id === currentStepId;
    const isHighlighted = focusStepId ? highlighted.has(step.id) : false;

    return {
      id: step.id,
      type: "planStep",
      position,
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
      ...(parentScopeId ? { parentId: parentScopeId, extent: "parent" as const } : {}),
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

  return { nodes: [...scopeNodes, ...nodes], edges };
}
