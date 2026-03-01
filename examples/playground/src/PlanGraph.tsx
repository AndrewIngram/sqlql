import { memo, useEffect, useMemo } from "react";
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
import type { QueryExecutionPlanStep, QueryStepState } from "sqlql";

import { cn } from "@/lib/utils";
import {
  buildPlanGraphLayout,
  buildPlanGraphModel,
  type PlanNodeData,
} from "@/plan-graph-model";

interface PlanGraphProps {
  steps: QueryExecutionPlanStep[];
  statesById: Record<string, QueryStepState | undefined>;
  currentStepId: string | null;
  selectedStepId: string | null;
  isVisible?: boolean;
  onSelectStep(stepId: string): void;
  onClearSelection?(): void;
  heightClassName?: string;
}

function isDomainInteractingStep(step: QueryExecutionPlanStep): boolean {
  return step.phase === "fetch" || step.kind === "scan" || step.kind === "aggregate";
}

const StepNode = memo(function StepNode({ data }: NodeProps): React.JSX.Element {
  const stepData = data as PlanNodeData;
  const isDomainStep = isDomainInteractingStep(stepData.step);

  return (
    <div
      className={cn(
        "h-[170px] w-[320px] rounded-xl border p-3 text-xs shadow-sm transition",
        isDomainStep ? "border-emerald-300 bg-emerald-50" : "bg-white",
        stepData.isSelected && "border-sky-600 ring-2 ring-sky-200",
        !stepData.isSelected && stepData.isHighlighted && "border-sky-300",
        !stepData.isHighlighted && !stepData.isSelected && "border-slate-200",
      )}
    >
      <Handle
        type="target"
        position={Position.Left}
        className={cn("!h-2 !w-2", isDomainStep ? "!bg-emerald-500" : "!bg-slate-400")}
      />
      <Handle
        type="source"
        position={Position.Right}
        className={cn("!h-2 !w-2", isDomainStep ? "!bg-emerald-500" : "!bg-slate-400")}
      />

      <div className="mb-2">
        <div className="truncate font-mono text-[11px] font-semibold text-slate-700">
          {stepData.step.id}
        </div>
      </div>

      <div className="mb-1 text-sm font-semibold text-slate-900">{stepData.step.kind}</div>
      <div className="mb-2 text-[11px] uppercase tracking-wide text-slate-500">
        {isDomainStep ? "domain call" : "internal op"}
      </div>
      <div className="mb-2 text-[11px] uppercase tracking-wide text-slate-500">{stepData.step.phase}</div>
      <div className="line-clamp-3 text-[12px] text-slate-700">{stepData.step.summary}</div>
    </div>
  );
});

const nodeTypes: NodeTypes = {
  planStep: StepNode,
};

interface PlanGraphCanvasProps extends PlanGraphProps {}

function PlanGraphCanvas({
  steps,
  statesById,
  currentStepId,
  selectedStepId,
  isVisible = true,
  onSelectStep,
  onClearSelection,
  heightClassName,
}: PlanGraphCanvasProps): React.JSX.Element {
  const graphLayout = useMemo(() => buildPlanGraphLayout(steps), [steps]);
  const nodesInitialized = useNodesInitialized();

  const graphModel = useMemo(
    () => buildPlanGraphModel(graphLayout, statesById, selectedStepId, currentStepId),
    [currentStepId, graphLayout, selectedStepId, statesById],
  );

  const { fitView, setCenter } = useReactFlow();

  useEffect(() => {
    if (!isVisible || !nodesInitialized || graphModel.nodes.length === 0) {
      return;
    }

    const frame = window.requestAnimationFrame(() => {
      void fitView({
        padding: 0.16,
        duration: 260,
        minZoom: 0.35,
        maxZoom: 1.15,
      });
    });
    const timeoutId = window.setTimeout(() => {
      void fitView({
        padding: 0.16,
        duration: 0,
        minZoom: 0.35,
        maxZoom: 1.15,
      });
    }, 140);

    return () => {
      window.cancelAnimationFrame(frame);
      window.clearTimeout(timeoutId);
    };
  }, [fitView, graphLayout, graphModel.nodes.length, isVisible, nodesInitialized]);

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
      duration: 240,
      zoom: 0.9,
    });
  }, [graphModel.nodes, isVisible, selectedStepId, setCenter]);

  if (steps.length === 0) {
    return <div className="text-sm text-slate-500">Run or step a query to populate the execution plan.</div>;
  }

  return (
    <div
      className={cn(
        "relative overflow-hidden rounded-lg border border-dashed border-slate-300 bg-slate-50",
        heightClassName ?? "h-[420px]",
      )}
    >
      <div className="pointer-events-none absolute left-3 top-3 z-10 flex gap-3 rounded-md border bg-white/90 px-3 py-1 text-[11px] text-slate-600 shadow-sm">
        <div className="inline-flex items-center gap-1.5">
          <span className="h-2 w-2 rounded-full bg-emerald-500" />
          Domain call
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
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable
        minZoom={0.25}
        maxZoom={1.5}
        onNodeClick={(_event, node) => onSelectStep(node.id)}
        onPaneClick={() => onClearSelection?.()}
      >
        <Controls position="top-right" showInteractive={false} />
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
