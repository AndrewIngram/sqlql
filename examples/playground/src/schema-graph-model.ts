import dagre from "dagre";
import { MarkerType, Position, type Edge, type Node } from "@xyflow/react";
import type { SchemaDefinition, SqlScalarType, TableColumnDefinition } from "@tupl/schema";

const NODE_WIDTH = 380;
const HEADER_HEIGHT = 42;
const ROW_HEIGHT = 28;
const FOOTER_HEIGHT = 8;

interface LayoutPosition {
  x: number;
  y: number;
}

export interface SchemaTableColumn {
  name: string;
  type: SqlScalarType;
  nullable: boolean;
}

export interface SchemaRelationNodeData extends Record<string, unknown> {
  tableName: string;
  columns: SchemaTableColumn[];
  primaryKeyColumns: string[];
  foreignKeyCount: number;
  isSelected: boolean;
}

export interface SchemaRelationEdge {
  sourceTable: string;
  sourceColumn: string;
  targetTable: string;
  targetColumn: string;
}

export interface SchemaGraphLayout {
  tableOrder: string[];
  positionsById: Map<string, LayoutPosition>;
  edges: SchemaRelationEdge[];
}

export interface SchemaGraphModel {
  nodes: Array<Node<SchemaRelationNodeData>>;
  edges: Edge[];
}

export function schemaHandleId(side: "in" | "out", columnName: string): string {
  return `${side}:${encodeURIComponent(columnName)}`;
}

function resolveColumn(definition: TableColumnDefinition): {
  type: SqlScalarType;
  nullable: boolean;
} {
  if (typeof definition === "string") {
    return {
      type: definition,
      nullable: true,
    };
  }

  return {
    type: definition.type,
    nullable: definition.nullable ?? true,
  };
}

function rowHeightForTable(schema: SchemaDefinition, tableName: string): number {
  const table = schema.tables[tableName];
  const columnCount = table ? Object.keys(table.columns).length : 0;
  return HEADER_HEIGHT + columnCount * ROW_HEIGHT + FOOTER_HEIGHT;
}

function readTableForeignKeys(
  schema: SchemaDefinition,
  tableName: string,
): Array<{
  sourceColumn: string;
  targetTable: string;
  targetColumn: string;
}> {
  const table = schema.tables[tableName];
  if (!table) {
    return [];
  }

  const out: Array<{
    sourceColumn: string;
    targetTable: string;
    targetColumn: string;
  }> = [];

  for (const [columnName, columnDefinition] of Object.entries(table.columns)) {
    if (typeof columnDefinition === "string" || !columnDefinition.foreignKey) {
      continue;
    }

    out.push({
      sourceColumn: columnName,
      targetTable: columnDefinition.foreignKey.table,
      targetColumn: columnDefinition.foreignKey.column,
    });
  }

  for (const foreignKey of table.constraints?.foreignKeys ?? []) {
    for (let index = 0; index < foreignKey.columns.length; index += 1) {
      const sourceColumn = foreignKey.columns[index];
      const targetColumn = foreignKey.references.columns[index];
      if (!sourceColumn || !targetColumn) {
        continue;
      }

      out.push({
        sourceColumn,
        targetTable: foreignKey.references.table,
        targetColumn,
      });
    }
  }

  const unique = new Map<
    string,
    { sourceColumn: string; targetTable: string; targetColumn: string }
  >();
  for (const entry of out) {
    const key = `${entry.sourceColumn}->${entry.targetTable}.${entry.targetColumn}`;
    if (!unique.has(key)) {
      unique.set(key, entry);
    }
  }

  return [...unique.values()];
}

function readTablePrimaryKeyColumns(schema: SchemaDefinition, tableName: string): string[] {
  const table = schema.tables[tableName];
  if (!table) {
    return [];
  }

  if (table.constraints?.primaryKey) {
    return [...table.constraints.primaryKey.columns];
  }

  return Object.entries(table.columns)
    .filter(([, columnDefinition]) => {
      return typeof columnDefinition !== "string" && columnDefinition.primaryKey === true;
    })
    .map(([columnName]) => columnName);
}

export function buildSchemaGraphLayout(schema: SchemaDefinition): SchemaGraphLayout {
  const graph = new dagre.graphlib.Graph();
  graph.setDefaultEdgeLabel(() => ({}));
  graph.setGraph({
    rankdir: "LR",
    ranksep: 210,
    nodesep: 88,
    marginx: 24,
    marginy: 24,
  });

  const tableOrder = Object.keys(schema.tables);
  const edges: SchemaRelationEdge[] = [];

  for (const tableName of tableOrder) {
    graph.setNode(tableName, {
      width: NODE_WIDTH,
      height: rowHeightForTable(schema, tableName),
    });
  }

  for (const tableName of Object.keys(schema.tables)) {
    for (const foreignKey of readTableForeignKeys(schema, tableName)) {
      if (!schema.tables[foreignKey.targetTable]) {
        continue;
      }

      graph.setEdge(tableName, foreignKey.targetTable);
      edges.push({
        sourceTable: tableName,
        sourceColumn: foreignKey.sourceColumn,
        targetTable: foreignKey.targetTable,
        targetColumn: foreignKey.targetColumn,
      });
    }
  }

  dagre.layout(graph);

  const positionsById = new Map<string, LayoutPosition>();
  for (const tableName of tableOrder) {
    const layoutNode = graph.node(tableName);
    positionsById.set(tableName, {
      x: layoutNode.x - NODE_WIDTH / 2,
      y: layoutNode.y - rowHeightForTable(schema, tableName) / 2,
    });
  }

  return {
    tableOrder,
    positionsById,
    edges,
  };
}

export function buildSchemaGraphModel(
  schema: SchemaDefinition,
  layout: SchemaGraphLayout,
  selectedTableName: string | null,
): SchemaGraphModel {
  const nodes: Array<Node<SchemaRelationNodeData>> = layout.tableOrder.map((tableName) => {
    const table = schema.tables[tableName];
    const position = layout.positionsById.get(tableName);

    if (!table) {
      throw new Error(`Schema graph table missing: ${tableName}`);
    }

    const columns = Object.entries(table.columns).map(([columnName, definition]) => {
      const resolved = resolveColumn(definition);
      return {
        name: columnName,
        type: resolved.type,
        nullable: resolved.nullable,
      };
    });

    return {
      id: tableName,
      type: "schemaTable",
      position: position ?? { x: 0, y: 0 },
      data: {
        tableName,
        columns,
        primaryKeyColumns: readTablePrimaryKeyColumns(schema, tableName),
        foreignKeyCount: readTableForeignKeys(schema, tableName).length,
        isSelected: selectedTableName === tableName,
      },
      targetPosition: Position.Left,
      sourcePosition: Position.Right,
      draggable: false,
      selectable: true,
    };
  });

  const edges: Edge[] = layout.edges.map((edge, index) => ({
    id: `fk:${edge.sourceTable}.${edge.sourceColumn}->${edge.targetTable}.${edge.targetColumn}:${index}`,
    source: edge.sourceTable,
    target: edge.targetTable,
    sourceHandle: schemaHandleId("out", edge.sourceColumn),
    targetHandle: schemaHandleId("in", edge.targetColumn),
    markerEnd: {
      type: MarkerType.ArrowClosed,
      width: 16,
      height: 16,
    },
    style: {
      stroke: "#475569",
      strokeWidth: 1.6,
    },
  }));

  return {
    nodes,
    edges,
  };
}
