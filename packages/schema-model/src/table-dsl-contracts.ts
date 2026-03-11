/**
 * Table DSL contracts is the curated schema-model surface for table/view declaration and builder contracts.
 * Internal modules should prefer the narrower definition, typed-column, and builder contract files.
 */
export type {
  DslTableDefinition,
  DslViewDefinition,
  SchemaCalculatedColumnDefinition,
  SchemaColumnLensDefinition,
  SchemaDslRelationRef,
  SchemaTypedColumnDefinition,
} from "./table-definition-contracts";
export type { SchemaTypedColumnBuilder } from "./typed-column-builder-contracts";
export type { SchemaBuilder } from "./schema-builder-contracts";
