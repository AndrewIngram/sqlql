import { Result } from "better-result";

import type {
  AggregateFunctionAst,
  CteAst,
  ExpressionAst,
  FromEntryAst,
  FunctionAst,
  LimitAst,
  OrderByTermAst,
  SelectAst,
  SelectColumnAst,
  WindowClauseEntryAst,
  WindowOverAst,
  WindowSpecificationAst,
} from "./ast";
import { TuplParseError } from "@tupl/foundation";
import { tokenizeSql, type Token } from "./lexer";

type BinaryOperatorSpec =
  | {
      kind: "simple";
      operator: string;
      precedence: number;
    }
  | {
      kind: "between";
      precedence: number;
    }
  | {
      kind: "in";
      precedence: number;
      negated?: boolean;
    }
  | {
      kind: "is";
      precedence: number;
    }
  | {
      kind: "like";
      precedence: number;
      negated?: boolean;
    }
  | {
      kind: "is_distinct";
      precedence: number;
      negated?: boolean;
    };

export function parseSqliteSelectAst(sql: string): SelectAst {
  const result = parseSqliteSelectAstResult(sql);
  if (Result.isError(result)) {
    throw result.error;
  }
  return result.value;
}

export function parseSqliteSelectAstResult(sql: string) {
  return Result.try({
    try: () => parseSqliteSelectAstOrThrow(sql),
    catch: (error) =>
      new TuplParseError({
        sql,
        message: error instanceof Error ? error.message : String(error),
        cause: error,
      }),
  });
}

function parseSqliteSelectAstOrThrow(sql: string): SelectAst {
  const parser = new SqliteSelectParser(sql);
  const ast = parser.parseStatement();
  parser.consumeStatementTerminator();
  return ast;
}

class SqliteSelectParser {
  readonly #tokens: Token[];
  #index = 0;

  constructor(sql: string) {
    this.#tokens = tokenizeSql(sql);
  }

  parseStatement(): SelectAst {
    const start = this.current();
    if (
      start.kind === "keyword" &&
      (start.upper === "UPDATE" || start.upper === "INSERT" || start.upper === "DELETE")
    ) {
      throw new Error("Only SELECT statements are currently supported.");
    }

    let withClause: CteAst[] | undefined;
    if (this.matchKeyword("WITH")) {
      withClause = this.parseWithClause();
    }

    if (!this.matchKeyword("SELECT")) {
      throw new Error("Only SELECT statements are currently supported.");
    }

    const root = this.parseSelectCoreAfterSelect();
    if (withClause && withClause.length > 0) {
      root.with = withClause;
    }

    this.parseSetOperations(root);
    return root;
  }

  consumeStatementTerminator(): void {
    if (this.matchSymbol(";")) {
      while (this.matchSymbol(";")) {
        // allow repeated trailing semicolons
      }
    }

    if (this.current().kind !== "eof") {
      throw new Error("Only a single SQL statement is supported.");
    }
  }

  parseWithClause(): CteAst[] {
    const recursive = this.matchKeyword("RECURSIVE");
    const entries: CteAst[] = [];

    while (true) {
      const cteName = this.parseIdentifier();

      if (this.matchSymbol("(")) {
        if (!this.matchSymbol(")")) {
          while (true) {
            this.parseIdentifier();
            if (!this.matchSymbol(",")) {
              break;
            }
          }
          this.expectSymbol(")");
        }
      }

      this.expectKeyword("AS");
      this.expectSymbol("(");
      const statement = this.parseStatement();
      this.expectSymbol(")");

      entries.push({
        name: { value: cteName },
        stmt: {
          ast: statement,
        },
        ...(recursive ? { recursive: true } : {}),
      });

      if (!this.matchSymbol(",")) {
        break;
      }
    }

    return entries;
  }

  parseSetOperations(root: SelectAst): void {
    let cursor = root;
    while (true) {
      const operation = this.tryParseSetOperator();
      if (!operation) {
        return;
      }

      this.expectKeyword("SELECT");
      const next = this.parseSelectCoreAfterSelect();
      cursor.set_op = operation;
      cursor._next = next;
      cursor = next;
    }
  }

  tryParseSetOperator(): string | null {
    if (this.matchKeyword("UNION")) {
      if (this.matchKeyword("ALL")) {
        return "UNION ALL";
      }
      return "UNION";
    }

    if (this.matchKeyword("INTERSECT")) {
      return "INTERSECT";
    }

    if (this.matchKeyword("EXCEPT")) {
      return "EXCEPT";
    }

    return null;
  }

  parseSelectCoreAfterSelect(): SelectAst {
    const ast: SelectAst = {
      type: "select",
    };

    if (this.matchKeyword("DISTINCT")) {
      ast.distinct = "DISTINCT";
    }

    ast.columns = this.parseSelectColumns();

    if (this.matchKeyword("FROM")) {
      ast.from = this.parseFromClause();
    }

    if (this.matchKeyword("WHERE")) {
      ast.where = this.parseExpression();
    }

    if (this.matchKeyword("GROUP")) {
      this.expectKeyword("BY");
      ast.groupby = {
        columns: this.parseExpressionList(),
      };
    }

    if (this.matchKeyword("HAVING")) {
      ast.having = this.parseExpression();
    }

    if (this.matchKeyword("WINDOW")) {
      ast.window = this.parseWindowClause();
    }

    if (this.matchKeyword("ORDER")) {
      this.expectKeyword("BY");
      ast.orderby = this.parseOrderByTerms();
    }

    if (this.matchKeyword("LIMIT")) {
      ast.limit = this.parseLimitClause();
    }

    return ast;
  }

  parseSelectColumns(): "*" | SelectColumnAst[] {
    if (this.matchSymbol("*")) {
      return "*";
    }

    const columns: SelectColumnAst[] = [];
    while (true) {
      const expr = this.parseExpression();
      let alias: string | undefined;

      if (this.matchKeyword("AS")) {
        alias = this.parseIdentifier();
      } else if (this.current().kind === "identifier") {
        alias = this.consume().text;
      }

      columns.push({
        expr,
        ...(alias ? { as: alias } : {}),
      });

      if (!this.matchSymbol(",")) {
        break;
      }
    }

    return columns;
  }

  parseFromClause(): FromEntryAst[] {
    const entries: FromEntryAst[] = [];
    entries.push(this.parseFromSource());

    while (true) {
      const joinType = this.tryParseJoinType();
      if (!joinType) {
        break;
      }

      const entry = this.parseFromSource();
      entry.join = joinType;
      this.expectKeyword("ON");
      entry.on = this.parseExpression();
      entries.push(entry);
    }

    return entries;
  }

  parseFromSource(): FromEntryAst {
    if (this.matchSymbol("(")) {
      if (!this.isSelectStart(this.current())) {
        throw new Error("Unsupported FROM clause entry.");
      }
      const statement = this.parseStatement();
      this.expectSymbol(")");
      const alias = this.parseOptionalAlias();
      return {
        stmt: {
          ast: statement,
        },
        ...(alias ? { as: alias } : {}),
      };
    }

    const table = this.parseIdentifier();
    const alias = this.parseOptionalAlias();

    return {
      table,
      ...(alias && alias !== table ? { as: alias } : {}),
    };
  }

  parseOptionalAlias(): string | undefined {
    if (this.matchKeyword("AS")) {
      return this.parseIdentifier();
    }

    const token = this.current();
    if (token.kind !== "identifier") {
      return undefined;
    }

    return this.consume().text;
  }

  tryParseJoinType(): string | null {
    if (this.matchKeyword("JOIN")) {
      return "JOIN";
    }

    const next = this.current();
    if (next.kind !== "keyword") {
      return null;
    }

    if (
      next.upper !== "INNER" &&
      next.upper !== "LEFT" &&
      next.upper !== "RIGHT" &&
      next.upper !== "FULL"
    ) {
      return null;
    }

    const side = this.consume().upper;
    const hasOuter = this.matchKeyword("OUTER");
    this.expectKeyword("JOIN");
    return hasOuter ? `${side} OUTER JOIN` : `${side} JOIN`;
  }

  parseWindowClause(): WindowClauseEntryAst[] {
    const entries: WindowClauseEntryAst[] = [];

    while (true) {
      const name = this.parseIdentifier();
      this.expectKeyword("AS");
      this.expectSymbol("(");
      const specification = this.parseWindowSpecification();
      this.expectSymbol(")");

      entries.push({
        name,
        as_window_specification: {
          window_specification: specification,
        },
      });

      if (!this.matchSymbol(",")) {
        break;
      }
    }

    return entries;
  }

  parseOrderByTerms(): OrderByTermAst[] {
    const terms: OrderByTermAst[] = [];
    while (true) {
      const expr = this.parseExpression();
      let direction: "ASC" | "DESC" | undefined;

      if (this.matchKeyword("ASC")) {
        direction = "ASC";
      } else if (this.matchKeyword("DESC")) {
        direction = "DESC";
      }

      terms.push({
        expr,
        ...(direction ? { type: direction } : {}),
      });

      if (!this.matchSymbol(",")) {
        break;
      }
    }

    return terms;
  }

  parseLimitClause(): LimitAst {
    const first = this.parseLimitNumeric();
    if (this.matchSymbol(",")) {
      const second = this.parseLimitNumeric();
      return {
        value: [{ value: first }, { value: second }],
        seperator: ",",
      };
    }

    if (this.matchKeyword("OFFSET")) {
      const second = this.parseLimitNumeric();
      return {
        value: [{ value: first }, { value: second }],
        seperator: "offset",
      };
    }

    return {
      value: [{ value: first }],
    };
  }

  parseLimitNumeric(): number {
    const token = this.consume();
    if (token.kind === "number") {
      const parsed = Number(token.text);
      if (Number.isFinite(parsed)) {
        return parsed;
      }
    }
    throw new Error("Unable to parse LIMIT value.");
  }

  parseExpression(minPrecedence = 0): ExpressionAst {
    let left = this.parsePrefixExpression();

    while (true) {
      const spec = this.peekBinaryOperator();
      if (!spec || spec.precedence < minPrecedence) {
        break;
      }

      if (spec.kind === "between") {
        this.expectKeyword("BETWEEN");
        const low = this.parseExpression(spec.precedence + 1);
        this.expectKeyword("AND");
        const high = this.parseExpression(spec.precedence + 1);
        left = {
          type: "binary_expr",
          operator: "BETWEEN",
          left,
          right: {
            type: "expr_list",
            value: [low, high],
          },
        };
        continue;
      }

      if (spec.kind === "in") {
        if (spec.negated) {
          this.expectKeyword("NOT");
        }
        this.expectKeyword("IN");
        left = {
          type: "binary_expr",
          operator: spec.negated ? "NOT IN" : "IN",
          left,
          right: this.parseInRightSide(),
        };
        continue;
      }

      if (spec.kind === "is") {
        this.expectKeyword("IS");
        const operator = this.matchKeyword("NOT") ? "IS NOT" : "IS";
        const right = this.parseExpression(spec.precedence + 1);
        left = {
          type: "binary_expr",
          operator,
          left,
          right,
        };
        continue;
      }

      if (spec.kind === "like") {
        if (spec.negated) {
          this.expectKeyword("NOT");
        }
        this.expectKeyword("LIKE");
        const right = this.parseExpression(spec.precedence + 1);
        left = {
          type: "binary_expr",
          operator: spec.negated ? "NOT LIKE" : "LIKE",
          left,
          right,
        };
        continue;
      }

      if (spec.kind === "is_distinct") {
        this.expectKeyword("IS");
        if (spec.negated) {
          this.expectKeyword("NOT");
        }
        this.expectKeyword("DISTINCT");
        this.expectKeyword("FROM");
        const right = this.parseExpression(spec.precedence + 1);
        left = {
          type: "binary_expr",
          operator: spec.negated ? "IS NOT DISTINCT FROM" : "IS DISTINCT FROM",
          left,
          right,
        };
        continue;
      }

      this.consumeBinaryOperatorToken(spec.operator);
      const right = this.parseExpression(spec.precedence + 1);
      left = {
        type: "binary_expr",
        operator: spec.operator,
        left,
        right,
      };
    }

    return left;
  }

  parseInRightSide(): ExpressionAst {
    this.expectSymbol("(");
    if (this.isSelectStart(this.current())) {
      const statement = this.parseStatement();
      this.expectSymbol(")");
      return {
        ast: statement,
      };
    }

    if (this.matchSymbol(")")) {
      return {
        type: "expr_list",
        value: [],
      };
    }

    const values = this.parseExpressionList();
    this.expectSymbol(")");
    return {
      type: "expr_list",
      value: values,
    };
  }

  parseExpressionList(): ExpressionAst[] {
    const values: ExpressionAst[] = [];
    while (true) {
      values.push(this.parseExpression());
      if (!this.matchSymbol(",")) {
        break;
      }
    }
    return values;
  }

  parsePrefixExpression(): ExpressionAst {
    if (this.matchKeyword("NOT")) {
      return {
        type: "function",
        name: {
          name: [{ value: "NOT" }],
        },
        args: {
          value: [this.parsePrefixExpression()],
        },
      };
    }

    if (this.matchKeyword("EXISTS")) {
      this.expectSymbol("(");
      const statement = this.parseStatement();
      this.expectSymbol(")");
      return {
        type: "function",
        name: {
          name: [{ value: "EXISTS" }],
        },
        args: {
          value: [
            {
              ast: statement,
            },
          ],
        },
      };
    }

    return this.parsePrimaryExpression();
  }

  parsePrimaryExpression(): ExpressionAst {
    if (this.matchSymbol("(")) {
      if (this.isSelectStart(this.current())) {
        const statement = this.parseStatement();
        this.expectSymbol(")");
        return {
          ast: statement,
          parentheses: true,
        };
      }

      const expr = this.parseExpression();
      this.expectSymbol(")");
      return markParenthesized(expr);
    }

    if (this.matchSymbol("*")) {
      return {
        type: "star",
        value: "*",
      };
    }

    const token = this.current();

    if (token.kind === "string") {
      this.consume();
      return {
        type: "string",
        value: token.text,
      };
    }

    if (token.kind === "number") {
      this.consume();
      return {
        type: "number",
        value: Number(token.text),
      };
    }

    if (this.matchKeyword("TRUE")) {
      return {
        type: "bool",
        value: true,
      };
    }

    if (this.matchKeyword("FALSE")) {
      return {
        type: "bool",
        value: false,
      };
    }

    if (this.matchKeyword("NULL")) {
      return {
        type: "null",
        value: null,
      };
    }

    if (this.matchKeyword("CASE")) {
      return this.parseCaseExpression();
    }

    if (this.isIdentifierLike(token)) {
      const nameToken = this.consume();
      const name = nameToken.text;
      const upperName = nameToken.upper;

      if (this.matchSymbol("(")) {
        return this.parseFunctionCall(upperName);
      }

      if (this.matchSymbol(".")) {
        let column: string;
        if (this.matchSymbol("*")) {
          column = "*";
        } else {
          column = this.parseIdentifier();
        }
        return {
          type: "column_ref",
          table: name,
          column,
        };
      }

      return {
        type: "column_ref",
        table: null,
        column: name,
      };
    }

    throw this.error(`Unexpected token "${token.text}" in expression.`);
  }

  parseFunctionCall(upperName: string): ExpressionAst {
    if (isAggregateFunctionName(upperName)) {
      return this.parseAggregateFunctionCall(upperName);
    }

    if (upperName === "CAST") {
      const value = this.parseExpression();
      this.expectKeyword("AS");
      const targetType = this.parseIdentifier();
      this.expectSymbol(")");
      return {
        type: "function",
        name: {
          name: [{ value: upperName }],
        },
        args: {
          value: [
            value,
            {
              type: "string",
              value: targetType,
            },
          ],
        },
      };
    }

    const args: ExpressionAst[] = [];
    if (!this.matchSymbol(")")) {
      while (true) {
        args.push(this.parseExpression());
        if (!this.matchSymbol(",")) {
          break;
        }
      }
      this.expectSymbol(")");
    }

    const node: FunctionAst = {
      type: "function",
      name: {
        name: [{ value: upperName }],
      },
      args: {
        value: args,
      },
    };

    if (this.matchKeyword("OVER")) {
      node.over = this.parseOverClause();
    }

    return node;
  }

  parseAggregateFunctionCall(name: string): AggregateFunctionAst {
    let argExpr: ExpressionAst | undefined;
    let distinct: "DISTINCT" | undefined;

    if (!this.matchSymbol(")")) {
      if (this.matchSymbol("*")) {
        argExpr = {
          type: "star",
          value: "*",
        };
      } else {
        if (this.matchKeyword("DISTINCT")) {
          distinct = "DISTINCT";
        }
        argExpr = this.parseExpression();
      }

      while (this.matchSymbol(",")) {
        // The current tupl aggregate support only consumes one argument;
        // preserve additional args by parsing through them.
        this.parseExpression();
      }
      this.expectSymbol(")");
    }

    const node: AggregateFunctionAst = {
      type: "aggr_func",
      name,
      ...(argExpr || distinct
        ? {
            args: {
              ...(argExpr ? { expr: argExpr } : {}),
              ...(distinct ? { distinct } : {}),
            },
          }
        : {}),
    };

    if (this.matchKeyword("OVER")) {
      node.over = this.parseOverClause();
    }

    return node;
  }

  parseOverClause(): WindowOverAst {
    if (!this.matchSymbol("(")) {
      const reference = this.parseIdentifier();
      return {
        as_window_specification: reference,
      };
    }

    const specification = this.parseWindowSpecification();
    this.expectSymbol(")");
    return {
      as_window_specification: {
        window_specification: specification,
      },
    };
  }

  parseWindowSpecification(): WindowSpecificationAst {
    const specification: WindowSpecificationAst = {};

    const token = this.current();
    if (
      this.isIdentifierLike(token) &&
      !this.currentIsKeyword("PARTITION") &&
      !this.currentIsKeyword("ORDER") &&
      !this.currentIsKeyword("ROWS") &&
      !this.currentIsKeyword("RANGE") &&
      !this.currentIsKeyword("GROUPS")
    ) {
      specification.name = this.parseIdentifier();
    }

    if (this.matchKeyword("PARTITION")) {
      this.expectKeyword("BY");
      specification.partitionby = this.parseExpressionList().map((expr) => ({ expr }));
    }

    if (this.matchKeyword("ORDER")) {
      this.expectKeyword("BY");
      specification.orderby = this.parseOrderByTerms().map((term) => ({
        expr: term.expr,
        ...(term.type ? { type: term.type } : {}),
      }));
    }

    if (this.isFrameClauseStart(this.current())) {
      specification.window_frame_clause = {
        raw: this.consumeFrameClauseRaw(),
      };
    }

    return specification;
  }

  parseCaseExpression(): ExpressionAst {
    const args: ExpressionAst[] = [];

    while (this.matchKeyword("WHEN")) {
      args.push(this.parseExpression());
      this.expectKeyword("THEN");
      args.push(this.parseExpression());
    }

    if (this.matchKeyword("ELSE")) {
      args.push(this.parseExpression());
    }

    this.expectKeyword("END");
    return {
      type: "function",
      name: {
        name: [{ value: "CASE" }],
      },
      args: {
        value: args,
      },
    };
  }

  consumeFrameClauseRaw(): string {
    const parts: string[] = [];
    let depth = 0;

    while (true) {
      const token = this.current();
      if (token.kind === "eof") {
        break;
      }

      if (token.kind === "symbol" && token.text === ")" && depth === 0) {
        break;
      }

      const consumed = this.consume();
      parts.push(consumed.text);
      if (consumed.kind === "symbol" && consumed.text === "(") {
        depth += 1;
      } else if (consumed.kind === "symbol" && consumed.text === ")") {
        depth -= 1;
      }
    }

    return parts.join(" ");
  }

  isFrameClauseStart(token: Token): boolean {
    return (
      token.kind === "keyword" &&
      (token.upper === "ROWS" || token.upper === "RANGE" || token.upper === "GROUPS")
    );
  }

  peekBinaryOperator(): BinaryOperatorSpec | null {
    if (this.currentIsKeyword("OR")) {
      return {
        kind: "simple",
        operator: "OR",
        precedence: 1,
      };
    }
    if (this.currentIsKeyword("AND")) {
      return {
        kind: "simple",
        operator: "AND",
        precedence: 2,
      };
    }
    if (this.currentIsKeyword("BETWEEN")) {
      return {
        kind: "between",
        precedence: 3,
      };
    }
    if (this.currentIsKeyword("NOT")) {
      const next = this.current(1);
      if (next?.kind === "keyword" && next.upper === "IN") {
        return {
          kind: "in",
          precedence: 3,
          negated: true,
        };
      }
      if (next?.kind === "keyword" && next.upper === "LIKE") {
        return {
          kind: "like",
          precedence: 3,
          negated: true,
        };
      }
    }
    if (this.currentIsKeyword("IN")) {
      return {
        kind: "in",
        precedence: 3,
      };
    }
    if (this.currentIsKeyword("LIKE")) {
      return {
        kind: "like",
        precedence: 3,
      };
    }
    if (this.currentIsKeyword("IS")) {
      const next = this.current(1);
      if (next?.kind === "keyword" && next.upper === "DISTINCT") {
        return {
          kind: "is_distinct",
          precedence: 3,
        };
      }
      if (
        next?.kind === "keyword" &&
        next.upper === "NOT" &&
        this.current(2)?.kind === "keyword" &&
        this.current(2).upper === "DISTINCT"
      ) {
        return {
          kind: "is_distinct",
          precedence: 3,
          negated: true,
        };
      }
      return {
        kind: "is",
        precedence: 3,
      };
    }

    const token = this.current();
    if (token.kind === "operator") {
      if (
        token.text === "=" ||
        token.text === "!=" ||
        token.text === "<>" ||
        token.text === ">" ||
        token.text === ">=" ||
        token.text === "<" ||
        token.text === "<="
      ) {
        return {
          kind: "simple",
          operator: token.text,
          precedence: 3,
        };
      }

      if (token.text === "+" || token.text === "-") {
        return {
          kind: "simple",
          operator: token.text,
          precedence: 4,
        };
      }

      if (token.text === "||") {
        return {
          kind: "simple",
          operator: token.text,
          precedence: 4,
        };
      }

      if (token.text === "/" || token.text === "%") {
        return {
          kind: "simple",
          operator: token.text,
          precedence: 5,
        };
      }
    }

    if (token.kind === "symbol" && token.text === "*") {
      return {
        kind: "simple",
        operator: "*",
        precedence: 5,
      };
    }

    return null;
  }

  consumeBinaryOperatorToken(operator: string): void {
    if (operator === "*" && this.matchSymbol("*")) {
      return;
    }

    if (operator === "AND" || operator === "OR") {
      this.expectKeyword(operator);
      return;
    }

    const token = this.current();
    if (token.kind === "operator" && token.text === operator) {
      this.consume();
      return;
    }

    throw this.error(`Expected operator "${operator}" but found "${token.text}".`);
  }

  isSelectStart(token: Token): boolean {
    return token.kind === "keyword" && (token.upper === "SELECT" || token.upper === "WITH");
  }

  isIdentifierLike(token: Token): boolean {
    return token.kind === "identifier" || token.kind === "keyword";
  }

  parseIdentifier(): string {
    const token = this.current();
    if (!this.isIdentifierLike(token)) {
      throw this.error(`Expected identifier but found "${token.text}".`);
    }
    this.consume();
    return token.text;
  }

  currentIsKeyword(keyword: string): boolean {
    const token = this.current();
    return token.kind === "keyword" && token.upper === keyword;
  }

  matchKeyword(keyword: string): boolean {
    if (!this.currentIsKeyword(keyword)) {
      return false;
    }
    this.consume();
    return true;
  }

  expectKeyword(keyword: string): void {
    if (!this.matchKeyword(keyword)) {
      throw this.error(`Expected keyword "${keyword}" but found "${this.current().text}".`);
    }
  }

  matchSymbol(symbol: string): boolean {
    const token = this.current();
    if (token.kind !== "symbol" || token.text !== symbol) {
      return false;
    }
    this.consume();
    return true;
  }

  expectSymbol(symbol: string): void {
    if (!this.matchSymbol(symbol)) {
      throw this.error(`Expected symbol "${symbol}" but found "${this.current().text}".`);
    }
  }

  current(offset = 0): Token {
    return this.#tokens[this.#index + offset] ?? this.#tokens[this.#tokens.length - 1]!;
  }

  consume(): Token {
    const token = this.current();
    if (token.kind !== "eof") {
      this.#index += 1;
    }
    return token;
  }

  error(message: string): Error {
    const token = this.current();
    return new Error(`${message} (at position ${token.index})`);
  }
}

function isAggregateFunctionName(name: string): boolean {
  return name === "COUNT" || name === "SUM" || name === "AVG" || name === "MIN" || name === "MAX";
}

function markParenthesized(expr: ExpressionAst): ExpressionAst {
  const maybe = expr as ExpressionAst & { type?: string; parentheses?: true };
  if (maybe.type == null) {
    return expr;
  }
  if (
    maybe.type === "binary_expr" ||
    maybe.type === "function" ||
    maybe.type === "aggr_func" ||
    maybe.type === "expr_list"
  ) {
    return {
      ...maybe,
      parentheses: true,
    };
  }
  return expr;
}
