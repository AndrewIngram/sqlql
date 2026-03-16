import { memo, useEffect, useMemo, useRef, useState } from "react";
import type React from "react";
import {
  Background,
  Controls,
  Handle,
  Position,
  ReactFlow,
  ReactFlowProvider,
  useNodesInitialized,
  useReactFlow,
  type NodeProps,
  type NodeTypes,
} from "@xyflow/react";
import type {
  QueryExecutionPlanScope,
  QueryExecutionPlanStep,
  QueryStepState,
} from "@tupl/runtime/session";

import { cn } from "./lib/utils";
import {
  buildPlanGraphLayout,
  buildPlanGraphModel,
  type PlanNodeData,
  type PlanScopeNodeData,
} from "./plan-graph-model";
import { presentStep } from "./plan-step-presentation";

interface PlanGraphProps {
  steps: QueryExecutionPlanStep[];
  scopes?: QueryExecutionPlanScope[];
  statesById: Record<string, QueryStepState | undefined>;
  currentStepId: string | null;
  selectedStepId: string | null;
  isVisible?: boolean;
  onSelectStep(this: void, stepId: string): void;
  onClearSelection?(this: void): void;
  heightClassName?: string;
  containerClassName?: string;
}

function classLabel(stepClass: ReturnType<typeof presentStep>["executionClass"]): string {
  switch (stepClass) {
    case "domain_call":
      return "domain call";
    case "local_over_fetched_rows":
      return "local over fetched rows";
    case "internal_op":
      return "internal op";
  }
}

function classContainerStyles(stepClass: ReturnType<typeof presentStep>["executionClass"]): string {
  switch (stepClass) {
    case "domain_call":
      return "border-emerald-300 bg-emerald-50";
    case "local_over_fetched_rows":
      return "border-amber-300 bg-amber-50";
    case "internal_op":
      return "bg-white";
  }
}

function classHandleStyles(stepClass: ReturnType<typeof presentStep>["executionClass"]): string {
  switch (stepClass) {
    case "domain_call":
      return "!bg-emerald-500";
    case "local_over_fetched_rows":
      return "!bg-amber-500";
    case "internal_op":
      return "!bg-slate-400";
  }
}

const StepNode = memo(function StepNode({ data }: NodeProps): React.JSX.Element {
  const stepData = data as PlanNodeData;
  const presentation = presentStep(stepData.step, stepData.state);
  const stepClass = presentation.executionClass;

  return (
    <div
      className={cn(
        "h-[170px] w-[320px] rounded-xl border p-3 text-xs shadow-sm transition",
        classContainerStyles(stepClass),
        stepData.isSelected && "border-sky-600 ring-2 ring-sky-200",
        !stepData.isSelected && stepData.isHighlighted && "border-sky-300",
        !stepData.isHighlighted && !stepData.isSelected && "border-slate-200",
      )}
    >
      <Handle
        type="target"
        position={Position.Left}
        className={cn("!h-2 !w-2", classHandleStyles(stepClass))}
      />
      <Handle
        type="source"
        position={Position.Right}
        className={cn("!h-2 !w-2", classHandleStyles(stepClass))}
      />

      <div className="mb-2 flex items-start justify-between gap-2">
        <div className="min-w-0">
          <div className="truncate text-sm font-semibold text-slate-900">
            {presentation.operator}
          </div>
          <div className="truncate font-mono text-[11px] font-semibold text-slate-600">
            {stepData.step.id}
          </div>
        </div>
        <div className="rounded border border-white/70 bg-white/80 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-slate-600">
          {presentation.clause}
        </div>
      </div>

      <div className="mb-2 flex flex-wrap gap-1.5">
        <span className="rounded-full bg-white/80 px-2 py-0.5 text-[10px] font-medium text-slate-700">
          {presentation.placement}
        </span>
        <span className="rounded-full bg-white/80 px-2 py-0.5 text-[10px] font-medium text-slate-700">
          {classLabel(stepClass)}
        </span>
      </div>

      <div className="mb-2 line-clamp-3 font-mono text-[11px] leading-5 text-slate-800">
        {presentation.signature}
      </div>

      {presentation.outputsPreview ? (
        <div className="mb-2 line-clamp-1 text-[11px] text-slate-600">
          <span className="font-semibold text-slate-700">out:</span> {presentation.outputsPreview}
        </div>
      ) : null}

      <div className="flex flex-wrap gap-1">
        {presentation.facts.map((fact) => (
          <span
            key={fact}
            className="rounded border border-white/70 bg-white/70 px-1.5 py-0.5 text-[10px] text-slate-600"
          >
            {fact}
          </span>
        ))}
      </div>
    </div>
  );
});

const ScopeNode = memo(function ScopeNode({ data }: NodeProps): React.JSX.Element {
  const scopeData = data as PlanScopeNodeData;

  return (
    <div className="pointer-events-none relative h-full w-full rounded-xl border border-slate-300/90 bg-slate-200/30">
      <div className="absolute left-3 top-2 rounded border border-slate-300 bg-slate-50 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-slate-600">
        {scopeData.scope.label}
      </div>
    </div>
  );
});

const nodeTypes: NodeTypes = {
  planStep: StepNode,
  planScope: ScopeNode,
};

interface PlanGraphCanvasProps extends PlanGraphProps {}

function PlanGraphCanvas({
  steps,
  scopes,
  statesById,
  currentStepId,
  selectedStepId,
  isVisible = true,
  onSelectStep,
  onClearSelection,
  heightClassName,
  containerClassName,
}: PlanGraphCanvasProps): React.JSX.Element {
  const graphLayout = useMemo(() => buildPlanGraphLayout(steps), [steps]);
  const nodesInitialized = useNodesInitialized();

  const graphModel = useMemo(
    () => buildPlanGraphModel(graphLayout, scopes, statesById, selectedStepId, currentStepId),
    [currentStepId, graphLayout, scopes, selectedStepId, statesById],
  );

  const { fitView, setCenter } = useReactFlow();
  const containerRef = useRef<HTMLDivElement | null>(null);
  const hasInitialFitRef = useRef(false);
  const [isContainerReady, setIsContainerReady] = useState(false);

  useEffect(() => {
    hasInitialFitRef.current = false;
  }, [graphLayout]);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) {
      return;
    }

    const updateReady = (): void => {
      const rect = container.getBoundingClientRect();
      setIsContainerReady(rect.width > 0 && rect.height > 0);
    };

    updateReady();

    if (typeof ResizeObserver === "undefined") {
      return;
    }

    const observer = new ResizeObserver(() => updateReady());
    observer.observe(container);

    return () => {
      observer.disconnect();
    };
  }, []);

  useEffect(() => {
    if (
      hasInitialFitRef.current ||
      !isVisible ||
      !nodesInitialized ||
      !isContainerReady ||
      graphModel.nodes.length === 0
    ) {
      return;
    }

    hasInitialFitRef.current = true;
    void fitView({
      padding: 0.16,
      duration: 0,
      minZoom: 0.35,
      maxZoom: 1.15,
    });
  }, [fitView, graphModel.nodes.length, isContainerReady, isVisible, nodesInitialized]);

  useEffect(() => {
    if (!isVisible || !selectedStepId) {
      return;
    }

    const focusedNode = graphModel.nodes.find((node) => node.id === selectedStepId);
    if (!focusedNode) {
      return;
    }

    const width = focusedNode.measured?.width ?? 320;
    const height = focusedNode.measured?.height ?? 170;

    void setCenter(focusedNode.position.x + width / 2, focusedNode.position.y + height / 2, {
      duration: 0,
      zoom: 0.9,
    });
  }, [graphModel.nodes, isVisible, selectedStepId, setCenter]);

  if (steps.length === 0) {
    return (
      <div className="text-sm text-slate-500">
        Run or step a query to populate the execution plan.
      </div>
    );
  }

  return (
    <div
      ref={containerRef}
      className={cn(
        "relative overflow-hidden rounded-lg border border-dashed border-slate-300 bg-slate-50",
        heightClassName ?? "h-[420px]",
        containerClassName,
      )}
    >
      <div className="pointer-events-none absolute left-3 top-3 z-10 flex gap-3 rounded-md border bg-white/90 px-3 py-1 text-[11px] text-slate-600 shadow-sm">
        <div className="inline-flex items-center gap-1.5">
          <span className="h-2 w-2 rounded-full bg-emerald-500" />
          Domain call
        </div>
        <div className="inline-flex items-center gap-1.5">
          <span className="h-2 w-2 rounded-full bg-amber-500" />
          Local over fetched rows
        </div>
        <div className="inline-flex items-center gap-1.5">
          <span className="h-2 w-2 rounded-full bg-slate-400" />
          Internal op
        </div>
      </div>
      <ReactFlow
        nodes={graphModel.nodes}
        edges={graphModel.edges}
        nodeTypes={nodeTypes}
        proOptions={{ hideAttribution: true }}
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable
        minZoom={0.25}
        maxZoom={1.5}
        onNodeClick={(_event, node) => {
          if (node.type !== "planStep") {
            return;
          }
          onSelectStep(node.id);
        }}
        onPaneClick={() => onClearSelection?.()}
      >
        <Controls position="bottom-left" orientation="horizontal" showInteractive={false} />
        <Background gap={24} size={1} color="#d6e3ef" />
      </ReactFlow>
    </div>
  );
}

export function PlanGraph(props: PlanGraphProps): React.JSX.Element {
  return (
    <ReactFlowProvider>
      <PlanGraphCanvas {...props} />
    </ReactFlowProvider>
  );
}
