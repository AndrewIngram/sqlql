import { memo, useEffect, useMemo } from "react";
import type React from "react";
import {
  Background,
  Controls,
  Handle,
  Position,
  ReactFlow,
  ReactFlowProvider,
  useReactFlow,
  type NodeProps,
  type NodeTypes,
} from "@xyflow/react";
import type { SchemaDefinition } from "sqlql";

import { cn } from "@/lib/utils";
import {
  buildSchemaGraphLayout,
  buildSchemaGraphModel,
  schemaHandleId,
  type SchemaRelationNodeData,
} from "@/schema-graph-model";

interface SchemaRelationsGraphProps {
  schema: SchemaDefinition;
  selectedTableName: string | null;
  onSelectTable(tableName: string): void;
  onClearSelection?(): void;
  heightClassName?: string;
  frameClassName?: string;
  embedded?: boolean;
}

const TableNode = memo(function TableNode({ data }: NodeProps): React.JSX.Element {
  const tableData = data as SchemaRelationNodeData;
  const primaryKeySet = new Set(tableData.primaryKeyColumns);

  return (
    <div
      className={cn(
        "overflow-visible rounded-xl border bg-white text-xs shadow-sm transition",
        tableData.isSelected ? "border-sky-600 ring-2 ring-sky-200" : "border-slate-200",
      )}
    >
      <div className="truncate border-b px-2 py-2 font-mono text-sm font-semibold text-slate-900">
        {tableData.tableName}
      </div>

      <div className="space-y-1 px-2 py-2">
        {tableData.columns.map((column) => (
          <div
            key={column.name}
            className="relative flex min-h-6 items-center justify-between rounded border border-slate-100 bg-slate-50 px-3 py-1"
          >
            <Handle
              type="target"
              position={Position.Left}
              id={schemaHandleId("in", column.name)}
              className="!h-2 !w-2 !bg-slate-500"
              style={{ top: "50%", transform: "translateY(-50%)", left: "-10px" }}
            />
            <div className="min-w-0 truncate font-mono text-[11px] text-slate-700">
              {column.name}
              {primaryKeySet.has(column.name) ? " *" : ""}
            </div>
            <div className="shrink-0 text-[11px] text-slate-500">
              {column.type}
              {column.nullable ? "?" : ""}
            </div>
            <Handle
              type="source"
              position={Position.Right}
              id={schemaHandleId("out", column.name)}
              className="!h-2 !w-2 !bg-slate-500"
              style={{ top: "50%", transform: "translateY(-50%)", right: "-10px" }}
            />
          </div>
        ))}
      </div>
    </div>
  );
});

const nodeTypes: NodeTypes = {
  schemaTable: TableNode,
};

function SchemaRelationsGraphCanvas({
  schema,
  selectedTableName,
  onSelectTable,
  onClearSelection,
  heightClassName,
  frameClassName,
  embedded = false,
}: SchemaRelationsGraphProps): React.JSX.Element {
  const layout = useMemo(() => buildSchemaGraphLayout(schema), [schema]);
  const model = useMemo(
    () => buildSchemaGraphModel(schema, layout, selectedTableName),
    [layout, schema, selectedTableName],
  );

  const { fitView, setCenter } = useReactFlow();

  useEffect(() => {
    if (model.nodes.length === 0) {
      return;
    }

    void fitView({ padding: 0.15, duration: 200, maxZoom: 1 });
  }, [fitView, layout, model.nodes.length]);

  useEffect(() => {
    if (!selectedTableName) {
      return;
    }

    const selectedNode = model.nodes.find((node) => node.id === selectedTableName);
    if (!selectedNode) {
      return;
    }

    void setCenter(selectedNode.position.x + 160, selectedNode.position.y + 90, {
      duration: 220,
      zoom: 0.9,
    });
  }, [model.nodes, selectedTableName, setCenter]);

  if (model.nodes.length === 0) {
    return <div className="text-sm text-slate-500">No tables in schema.</div>;
  }

  return (
    <div
      className={cn(
        embedded
          ? "overflow-hidden bg-slate-50"
          : "overflow-hidden rounded-lg border border-dashed border-slate-300 bg-slate-50",
        frameClassName,
        heightClassName ?? "h-[420px]",
      )}
    >
      <ReactFlow
        nodes={model.nodes}
        edges={model.edges}
        nodeTypes={nodeTypes}
        proOptions={{ hideAttribution: true }}
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable
        fitView
        minZoom={0.25}
        maxZoom={1.5}
        onNodeClick={(_event, node) => onSelectTable(node.id)}
        onPaneClick={() => onClearSelection?.()}
      >
        <Controls position="bottom-left" orientation="horizontal" showInteractive={false} />
        <Background gap={24} size={1} color="#d6e3ef" />
      </ReactFlow>
    </div>
  );
}

export function SchemaRelationsGraph(props: SchemaRelationsGraphProps): React.JSX.Element {
  return (
    <ReactFlowProvider>
      <SchemaRelationsGraphCanvas {...props} />
    </ReactFlowProvider>
  );
}
