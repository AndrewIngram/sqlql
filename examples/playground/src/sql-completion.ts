import type { SchemaDefinition } from "sqlql";
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

export function getSqlSuggestionLabels(
  sqlText: string,
  offset: number,
  schema: SchemaDefinition,
): {
  context: "table" | "column" | "alias_column" | "general";
  labels: string[];
} {
  const before = sqlText.slice(0, offset);
  const aliases = readAliases(sqlText);

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
    const table = schema.tables[tableName];
    return {
      context: "alias_column",
      labels: table ? Object.keys(table.columns) : [],
    };
  }

  const tableColumns = Object.entries(schema.tables).flatMap(([tableName, table]) =>
    Object.keys(table.columns).map((columnName) => `${tableName}.${columnName}`),
  );

  const aliasColumns = [...aliases.entries()].flatMap(([alias, tableName]) => {
    const table = schema.tables[tableName];
    if (!table) {
      return [];
    }

    return Object.keys(table.columns).map((columnName) => `${alias}.${columnName}`);
  });

  const plainColumns = Object.values(schema.tables).flatMap((table) => Object.keys(table.columns));

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

  return 5 as Monaco.languages.CompletionItemKind;
}

export function registerSqlCompletionProvider(
  monaco: typeof Monaco,
  getSchema: () => SchemaDefinition | null,
): Monaco.IDisposable {
  return monaco.languages.registerCompletionItemProvider("sql", {
    triggerCharacters: [".", " "],
    provideCompletionItems(model, position) {
      const schema = getSchema();
      if (!schema) {
        return { suggestions: [] };
      }

      const offset = model.getOffsetAt(position);
      const sqlText = model.getValue();
      const { context, labels } = getSqlSuggestionLabels(sqlText, offset, schema);
      const word = model.getWordUntilPosition(position);

      return {
        suggestions: labels.map((label) => ({
          label,
          kind: completionItemKindForLabel(label, context),
          insertText: label,
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
