export type TokenKind =
  | "identifier"
  | "keyword"
  | "string"
  | "number"
  | "symbol"
  | "operator"
  | "eof";

export interface Token {
  kind: TokenKind;
  text: string;
  upper: string;
  index: number;
}

const KEYWORDS = new Set([
  "ALL",
  "AND",
  "AS",
  "ASC",
  "BETWEEN",
  "BY",
  "CASE",
  "CAST",
  "COUNT",
  "CURRENT",
  "DENSE_RANK",
  "DESC",
  "DISTINCT",
  "ELSE",
  "END",
  "EXCEPT",
  "EXISTS",
  "FALSE",
  "FOLLOWING",
  "FROM",
  "FULL",
  "GROUP",
  "GROUPS",
  "HAVING",
  "INNER",
  "INTERSECT",
  "IN",
  "IS",
  "JOIN",
  "LAG",
  "LAST_VALUE",
  "LEAD",
  "LEFT",
  "LIKE",
  "LIMIT",
  "MAX",
  "MIN",
  "NOT",
  "NULL",
  "OFFSET",
  "ON",
  "OR",
  "ORDER",
  "OUTER",
  "OVER",
  "PARTITION",
  "PRECEDING",
  "RANGE",
  "RANK",
  "RECURSIVE",
  "RIGHT",
  "ROW",
  "ROW_NUMBER",
  "ROWS",
  "SELECT",
  "SUM",
  "THEN",
  "TRUE",
  "UNBOUNDED",
  "UNION",
  "UPDATE",
  "INSERT",
  "DELETE",
  "WHEN",
  "WHERE",
  "WINDOW",
  "WITH",
]);

const SYMBOLS = new Set(["(", ")", ",", ".", "*", ";"]);

export function tokenizeSql(input: string): Token[] {
  const tokens: Token[] = [];
  let index = 0;

  const push = (kind: TokenKind, text: string, at: number): void => {
    tokens.push({
      kind,
      text,
      upper: text.toUpperCase(),
      index: at,
    });
  };

  while (index < input.length) {
    const char = input[index];
    if (!char) {
      break;
    }

    if (isWhitespace(char)) {
      index += 1;
      continue;
    }

    if (char === "-" && input[index + 1] === "-") {
      index += 2;
      while (index < input.length && input[index] !== "\n") {
        index += 1;
      }
      continue;
    }

    if (char === "/" && input[index + 1] === "*") {
      index += 2;
      while (index < input.length && !(input[index] === "*" && input[index + 1] === "/")) {
        index += 1;
      }
      if (index >= input.length) {
        throw new Error("Unterminated block comment.");
      }
      index += 2;
      continue;
    }

    if (char === "'") {
      const start = index;
      index += 1;
      let value = "";
      while (index < input.length) {
        const ch = input[index];
        if (ch === "'") {
          if (input[index + 1] === "'") {
            value += "'";
            index += 2;
            continue;
          }
          index += 1;
          break;
        }
        value += ch ?? "";
        index += 1;
      }
      push("string", value, start);
      continue;
    }

    if (char === '"') {
      const start = index;
      index += 1;
      let value = "";
      while (index < input.length) {
        const ch = input[index];
        if (ch === '"') {
          if (input[index + 1] === '"') {
            value += '"';
            index += 2;
            continue;
          }
          index += 1;
          break;
        }
        value += ch ?? "";
        index += 1;
      }
      push("identifier", value, start);
      continue;
    }

    if (isDigit(char)) {
      const start = index;
      index += 1;
      while (index < input.length && isDigit(input[index] ?? "")) {
        index += 1;
      }
      if (input[index] === "." && isDigit(input[index + 1] ?? "")) {
        index += 1;
        while (index < input.length && isDigit(input[index] ?? "")) {
          index += 1;
        }
      }
      push("number", input.slice(start, index), start);
      continue;
    }

    if (char === "!" && input[index + 1] === "=") {
      push("operator", "!=", index);
      index += 2;
      continue;
    }
    if (char === "|" && input[index + 1] === "|") {
      push("operator", "||", index);
      index += 2;
      continue;
    }
    if (char === "<" && input[index + 1] === ">") {
      push("operator", "<>", index);
      index += 2;
      continue;
    }
    if (char === ">" && input[index + 1] === "=") {
      push("operator", ">=", index);
      index += 2;
      continue;
    }
    if (char === "<" && input[index + 1] === "=") {
      push("operator", "<=", index);
      index += 2;
      continue;
    }

    if (
      char === "=" ||
      char === ">" ||
      char === "<" ||
      char === "+" ||
      char === "-" ||
      char === "/" ||
      char === "%"
    ) {
      push("operator", char, index);
      index += 1;
      continue;
    }

    if (SYMBOLS.has(char)) {
      push("symbol", char, index);
      index += 1;
      continue;
    }

    if (isIdentifierStart(char)) {
      const start = index;
      index += 1;
      while (index < input.length && isIdentifierPart(input[index] ?? "")) {
        index += 1;
      }
      const text = input.slice(start, index);
      const upper = text.toUpperCase();
      if (KEYWORDS.has(upper)) {
        push("keyword", upper, start);
      } else {
        push("identifier", text, start);
      }
      continue;
    }

    throw new Error(`Unexpected character "${char}" at position ${index}.`);
  }

  tokens.push({
    kind: "eof",
    text: "<eof>",
    upper: "<EOF>",
    index: input.length,
  });
  return tokens;
}

function isWhitespace(char: string): boolean {
  return char === " " || char === "\n" || char === "\t" || char === "\r";
}

function isDigit(char: string): boolean {
  return char >= "0" && char <= "9";
}

function isIdentifierStart(char: string): boolean {
  return (char >= "A" && char <= "Z") || (char >= "a" && char <= "z") || char === "_";
}

function isIdentifierPart(char: string): boolean {
  return isIdentifierStart(char) || isDigit(char);
}
