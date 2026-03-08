import { type SchemaDefinition } from "@tupl/core";
import type * as Monaco from "monaco-editor";

const SQL_KEYWORDS = [
  "SELECT",
  "FROM",
  "WHERE",
  "JOIN",
  "LEFT JOIN",
  "RIGHT JOIN",
  "FULL JOIN",
  "ON",
  "GROUP BY",
  "ORDER BY",
  "LIMIT",
  "OFFSET",
  "WITH",
  "AS",
  "DISTINCT",
  "UNION",
  "INTERSECT",
  "EXCEPT",
  "HAVING",
  "AND",
  "OR",
  "NOT",
  "IN",
  "IS NULL",
  "IS NOT NULL",
  "OVER",
  "PARTITION BY",
];

const SQL_FUNCTIONS = ["COUNT", "SUM", "AVG", "MIN", "MAX", "ROW_NUMBER", "RANK", "DENSE_RANK"];

function readAliases(sqlText: string): Map<string, string> {
  const aliases = new Map<string, string>();
  const aliasRegex = /\b(?:from|join)\s+([a-z_][\w]*)\s*(?:as\s+)?([a-z_][\w]*)?/gi;

  let match = aliasRegex.exec(sqlText);
  while (match) {
    const tableName = match[1];
    if (tableName) {
      const alias = match[2] ?? tableName;
      aliases.set(alias.toLowerCase(), tableName);
    }
    match = aliasRegex.exec(sqlText);
  }

  return aliases;
}

function unique(values: string[]): string[] {
  return [...new Set(values)];
}

interface ResolvedColumnReference {
  tableName: string;
  columnName: string;
}

type ClauseContext = "where" | "order_by" | "general";

function resolveColumnReference(
  columnRef: string,
  aliases: Map<string, string>,
  schema: SchemaDefinition,
): ResolvedColumnReference | null {
  const parts = columnRef.split(".");
  if (parts.length === 2) {
    const qualifier = parts[0]?.toLowerCase();
    const columnName = parts[1];
    if (!qualifier || !columnName) {
      return null;
    }
    const tableName = aliases.get(qualifier) ?? qualifier;
    const table = schema.tables[tableName];
    if (!table || !(columnName in table.columns)) {
      return null;
    }
    return {
      tableName,
      columnName,
    };
  }

  if (parts.length !== 1) {
    return null;
  }

  const columnName = parts[0];
  if (!columnName) {
    return null;
  }

  const aliasTableNames = unique([...aliases.values()]);
  const candidateTableNames =
    aliasTableNames.length > 0 ? aliasTableNames : Object.keys(schema.tables);
  const matches = candidateTableNames.filter((tableName) => {
    const table = schema.tables[tableName];
    return Boolean(table && columnName in table.columns);
  });
  if (matches.length !== 1) {
    return null;
  }

  return {
    tableName: matches[0] ?? "",
    columnName,
  };
}

function resolveEnumSuggestions(
  before: string,
  aliases: Map<string, string>,
  schema: SchemaDefinition,
): string[] {
  const comparisonMatch =
    /([a-z_][\w]*(?:\.[a-z_][\w]*)?)\s*(?:=|!=|<>|<=|>=|<|>)\s*(?:'[^']*)?$/iu.exec(before);
  if (comparisonMatch?.[1]) {
    const resolved = resolveColumnReference(comparisonMatch[1], aliases, schema);
    if (resolved) {
      const column = schema.tables[resolved.tableName]?.columns[resolved.columnName];
      if (column && typeof column !== "string" && column.type === "text" && column.enum) {
        return column.enum.map((value) => `'${value.replaceAll("'", "''")}'`);
      }
    }
  }

  const inMatch = /([a-z_][\w]*(?:\.[a-z_][\w]*)?)\s+in\s*\((?:[^)]*)$/iu.exec(before);
  if (inMatch?.[1]) {
    const resolved = resolveColumnReference(inMatch[1], aliases, schema);
    if (resolved) {
      const column = schema.tables[resolved.tableName]?.columns[resolved.columnName];
      if (column && typeof column !== "string" && column.type === "text" && column.enum) {
        return column.enum.map((value) => `'${value.replaceAll("'", "''")}'`);
      }
    }
  }

  return [];
}

function lastKeywordIndex(haystack: string, pattern: RegExp): number {
  const globalPattern = new RegExp(pattern.source, `${pattern.flags.replaceAll("g", "")}g`);
  let index = -1;

  let match = globalPattern.exec(haystack);
  while (match) {
    if (match.index != null) {
      index = Math.max(index, match.index);
    }
    match = globalPattern.exec(haystack);
  }

  return index;
}

function detectClauseContext(before: string): ClauseContext {
  const whereIndex = lastKeywordIndex(before, /\bwhere\b/iu);
  const orderByIndex = lastKeywordIndex(before, /\border\s+by\b/iu);
  const whereEndIndex = Math.max(
    lastKeywordIndex(before, /\bgroup\s+by\b/iu),
    lastKeywordIndex(before, /\border\s+by\b/iu),
    lastKeywordIndex(before, /\blimit\b/iu),
    lastKeywordIndex(before, /\boffset\b/iu),
    lastKeywordIndex(before, /\bhaving\b/iu),
    lastKeywordIndex(before, /\bunion\b/iu),
    lastKeywordIndex(before, /\bintersect\b/iu),
    lastKeywordIndex(before, /\bexcept\b/iu),
  );
  const orderByEndIndex = Math.max(
    lastKeywordIndex(before, /\blimit\b/iu),
    lastKeywordIndex(before, /\boffset\b/iu),
    lastKeywordIndex(before, /\bunion\b/iu),
    lastKeywordIndex(before, /\bintersect\b/iu),
    lastKeywordIndex(before, /\bexcept\b/iu),
  );

  if (orderByIndex >= 0 && orderByIndex > orderByEndIndex) {
    return "order_by";
  }
  if (whereIndex >= 0 && whereIndex > whereEndIndex) {
    return "where";
  }

  return "general";
}

function isColumnAllowedInClause(
  schema: SchemaDefinition,
  tableName: string,
  columnName: string,
  _clauseContext: ClauseContext,
): boolean {
  const table = schema.tables[tableName];
  if (!table) {
    return false;
  }
  return columnName in table.columns;
}

function tableColumnNamesForClause(
  schema: SchemaDefinition,
  tableName: string,
  clauseContext: ClauseContext,
): string[] {
  const table = schema.tables[tableName];
  if (!table) {
    return [];
  }

  return Object.keys(table.columns).filter((columnName) =>
    isColumnAllowedInClause(schema, tableName, columnName, clauseContext),
  );
}

function isInsideSingleQuotedString(text: string): boolean {
  let inString = false;

  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    if (char !== "'") {
      continue;
    }

    const next = text[index + 1];
    if (inString && next === "'") {
      index += 1;
      continue;
    }

    inString = !inString;
  }

  return inString;
}

function unquoteSqlLiteral(value: string): string {
  if (value.startsWith("'") && value.endsWith("'") && value.length >= 2) {
    return value.slice(1, -1).replaceAll("''", "'");
  }
  return value;
}

export function getSqlSuggestionLabels(
  sqlText: string,
  offset: number,
  schema: SchemaDefinition,
): {
  context: "table" | "column" | "alias_column" | "enum_value" | "general";
  labels: string[];
} {
  const before = sqlText.slice(0, offset);
  const aliases = readAliases(sqlText);
  const clauseContext = detectClauseContext(before);

  if (/\b(?:from|join)\s+[\w]*$/i.test(before)) {
    return {
      context: "table",
      labels: Object.keys(schema.tables),
    };
  }

  const aliasDotMatch = /([a-z_][\w]*)\.([a-z_]*)$/i.exec(before);
  if (aliasDotMatch && aliasDotMatch[1]) {
    const alias = aliasDotMatch[1].toLowerCase();
    const tableName = aliases.get(alias) ?? alias;
    return {
      context: "alias_column",
      labels: tableColumnNamesForClause(schema, tableName, clauseContext),
    };
  }

  const enumSuggestions = resolveEnumSuggestions(before, aliases, schema);
  if (enumSuggestions.length > 0) {
    return {
      context: "enum_value",
      labels: enumSuggestions,
    };
  }

  const tableColumns = Object.keys(schema.tables).flatMap((tableName) =>
    tableColumnNamesForClause(schema, tableName, clauseContext).map(
      (columnName) => `${tableName}.${columnName}`,
    ),
  );

  const aliasColumns = [...aliases.entries()].flatMap(([alias, tableName]) => {
    return tableColumnNamesForClause(schema, tableName, clauseContext).map(
      (columnName) => `${alias}.${columnName}`,
    );
  });

  const plainColumns = Object.keys(schema.tables).flatMap((tableName) =>
    tableColumnNamesForClause(schema, tableName, clauseContext),
  );

  return {
    context: plainColumns.length > 0 ? "column" : "general",
    labels: unique([
      ...Object.keys(schema.tables),
      ...plainColumns,
      ...aliasColumns,
      ...tableColumns,
      ...SQL_FUNCTIONS,
      ...SQL_KEYWORDS,
    ]),
  };
}

function completionItemKindForLabel(
  label: string,
  context: string,
): Monaco.languages.CompletionItemKind {
  if (SQL_KEYWORDS.includes(label)) {
    return 17 as Monaco.languages.CompletionItemKind;
  }

  if (SQL_FUNCTIONS.includes(label)) {
    return 1 as Monaco.languages.CompletionItemKind;
  }

  if (context === "table") {
    return 7 as Monaco.languages.CompletionItemKind;
  }

  if (context === "enum_value") {
    return 12 as Monaco.languages.CompletionItemKind;
  }

  return 5 as Monaco.languages.CompletionItemKind;
}

export function registerSqlCompletionProvider(
  monaco: typeof Monaco,
  getSchema: () => SchemaDefinition | null,
): Monaco.IDisposable {
  return monaco.languages.registerCompletionItemProvider("sql", {
    triggerCharacters: [".", " ", "'", ",", "=", "("],
    provideCompletionItems(model, position) {
      const schema = getSchema();
      if (!schema) {
        return { suggestions: [] };
      }

      const offset = model.getOffsetAt(position);
      const sqlText = model.getValue();
      const { context, labels } = getSqlSuggestionLabels(sqlText, offset, schema);
      const word = model.getWordUntilPosition(position);
      const insideString =
        context === "enum_value" && isInsideSingleQuotedString(beforeText(sqlText, offset));

      return {
        suggestions: labels.map((label) => ({
          label,
          kind: completionItemKindForLabel(label, context),
          insertText: context === "enum_value" && insideString ? unquoteSqlLiteral(label) : label,
          range: {
            startLineNumber: position.lineNumber,
            endLineNumber: position.lineNumber,
            startColumn: word.startColumn,
            endColumn: word.endColumn,
          },
        })),
      };
    },
  });
}

function beforeText(sqlText: string, offset: number): string {
  return sqlText.slice(0, offset);
}
