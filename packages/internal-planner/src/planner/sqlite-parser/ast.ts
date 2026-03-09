export interface SelectAst {
  type: "select";
  with?: CteAst[];
  distinct?: "DISTINCT";
  columns?: "*" | SelectColumnAst[];
  from?: FromEntryAst[];
  where?: ExpressionAst;
  groupby?: GroupByAst;
  having?: ExpressionAst;
  orderby?: OrderByTermAst[];
  limit?: LimitAst;
  window?: WindowClauseEntryAst[];
  set_op?: string;
  _next?: SelectAst;
}

export interface CteAst {
  name: string | { value: string };
  stmt: {
    ast: SelectAst;
  };
  recursive?: boolean;
}

export interface FromEntryAst {
  table?: string;
  as?: string;
  join?: string;
  on?: ExpressionAst;
  stmt?: {
    ast: SelectAst;
  };
}

export interface SelectColumnAst {
  expr: ExpressionAst;
  as?: string;
}

export interface GroupByAst {
  columns: ExpressionAst[];
}

export interface OrderByTermAst {
  expr: ExpressionAst;
  type?: "ASC" | "DESC";
}

export interface LimitAst {
  value: Array<{ value: number }>;
  seperator?: "offset" | ",";
}

export type ExpressionAst =
  | ColumnRefAst
  | StarAst
  | StringLiteralAst
  | NumberLiteralAst
  | BooleanLiteralAst
  | NullLiteralAst
  | BinaryExpressionAst
  | FunctionAst
  | AggregateFunctionAst
  | ExprListAst
  | SubqueryAst;

export interface ColumnRefAst {
  type: "column_ref";
  table: string | null;
  column: string;
  parentheses?: true;
}

export interface StarAst {
  type: "star";
  value: "*";
  parentheses?: true;
}

export interface StringLiteralAst {
  type: "string";
  value: string;
  parentheses?: true;
}

export interface NumberLiteralAst {
  type: "number";
  value: number;
  parentheses?: true;
}

export interface BooleanLiteralAst {
  type: "bool";
  value: boolean;
  parentheses?: true;
}

export interface NullLiteralAst {
  type: "null";
  value: null;
  parentheses?: true;
}

export interface BinaryExpressionAst {
  type: "binary_expr";
  operator: string;
  left: ExpressionAst;
  right: ExpressionAst;
  parentheses?: true;
}

export interface FunctionAst {
  type: "function";
  name: {
    name: Array<{ value: string }>;
  };
  args?: {
    value?: ExpressionAst[] | ExpressionAst;
  };
  over?: WindowOverAst;
  parentheses?: true;
}

export interface AggregateFunctionAst {
  type: "aggr_func";
  name: string;
  args?: {
    expr?: ExpressionAst;
    distinct?: "DISTINCT";
  };
  over?: WindowOverAst;
  parentheses?: true;
}

export interface ExprListAst {
  type: "expr_list";
  value: ExpressionAst[];
  parentheses?: true;
}

export interface SubqueryAst {
  ast: SelectAst;
  parentheses?: true;
}

export interface WindowOverAst {
  as_window_specification: string | { window_specification: WindowSpecificationAst };
}

export interface WindowSpecificationAst {
  partitionby?: Array<{ expr: ExpressionAst }>;
  orderby?: Array<{ expr: ExpressionAst; type?: "ASC" | "DESC" }>;
  window_frame_clause?: {
    raw: string;
  };
  name?: string;
}

export interface WindowClauseEntryAst {
  name: string;
  as_window_specification: {
    window_specification: WindowSpecificationAst;
  };
}
