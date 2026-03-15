import { Result } from "better-result";

import { validateTableConstraintRows } from "../constraints";
import { TuplExecutionError } from "@tupl/foundation";
import {
  getDataEntityProvider,
  normalizeCapability,
  unwrapProviderOperationResult,
  type ProviderAdapter,
} from "@tupl/provider-kit";
import {
  type NormalizedPhysicalTableBinding,
  type QueryRow,
  type ScanFilterClause,
  type TableScanRequest,
} from "@tupl/schema-model";
import { mapProviderRowsToLogical } from "@tupl/schema-model/mapping";
import {
  createPhysicalBindingFromEntity,
  createTableDefinitionFromEntity,
  getNormalizedTableBinding,
  resolveNormalizedColumnSource,
  resolveTableProvider,
} from "@tupl/schema-model/normalization";

import {
  tryExecutionStep,
  tryExecutionStepAsync,
  type RelExecutionContext,
} from "./local-execution";
import { prefixRow, scanLocalRows } from "./row-ops";

/**
 * Scan execution owns provider-backed physical scans and explicit reads from materialized CTEs.
 */
export async function executeScanResult<TContext>(
  scan: Extract<import("@tupl/foundation").RelNode, { kind: "scan" }>,
  context: RelExecutionContext<TContext>,
) {
  const normalizedBinding = getNormalizedTableBinding(context.schema, scan.table);
  const providerNameSource =
    scan.entity?.provider ?? resolveTableProvider(context.schema, scan.table);
  const providerNameResult =
    typeof providerNameSource === "string" ? Result.ok(providerNameSource) : providerNameSource;
  if (Result.isError(providerNameResult)) {
    return providerNameResult;
  }
  const providerName = providerNameResult.value;
  const provider =
    context.providers[providerName] ??
    (scan.entity
      ? (getDataEntityProvider(scan.entity) as ProviderAdapter<TContext> | undefined)
      : undefined);
  if (!provider) {
    return Result.err(
      new TuplExecutionError({
        operation: "execute scan",
        message: `Missing provider adapter: ${providerName}`,
      }),
    );
  }

  const physicalBinding =
    normalizedBinding?.kind === "physical"
      ? normalizedBinding
      : scan.entity
        ? createPhysicalBindingFromEntity(scan.entity)
        : null;
  const tableDefinition =
    context.schema.tables[scan.table] ??
    (scan.entity ? createTableDefinitionFromEntity(scan.entity) : undefined);
  const requestResult = tryExecutionStep(
    "build provider scan request",
    () =>
      ({
        table: physicalBinding?.entity ?? scan.table,
        ...(scan.alias ? { alias: scan.alias } : {}),
        select: mapLogicalColumnsToSource(scan.select, physicalBinding),
        ...(scan.where ? { where: mapWhereToSource(scan.where, physicalBinding) } : {}),
        ...(scan.orderBy ? { orderBy: mapOrderToSource(scan.orderBy, physicalBinding) } : {}),
        ...(scan.limit != null ? { limit: scan.limit } : {}),
        ...(scan.offset != null ? { offset: scan.offset } : {}),
      }) satisfies TableScanRequest,
  );
  if (Result.isError(requestResult)) {
    return requestResult;
  }
  const request = requestResult.value;

  const providerRel = {
    ...scan,
    table: request.table,
    select: request.select,
    ...(request.where ? { where: request.where } : {}),
    ...(request.orderBy ? { orderBy: request.orderBy } : {}),
    ...(request.limit != null ? { limit: request.limit } : {}),
    ...(request.offset != null ? { offset: request.offset } : {}),
  };

  const capabilityResult = await tryExecutionStepAsync("check scan provider capability", () =>
    Promise.resolve(provider.canExecute(providerRel, context.context)),
  );
  if (Result.isError(capabilityResult)) {
    return capabilityResult;
  }
  const capability = normalizeCapability(capabilityResult.value);
  if (!capability.supported) {
    return Result.err(
      new TuplExecutionError({
        operation: "execute scan",
        message: `Provider ${providerName} cannot execute scan for table ${scan.table}${
          capability.reason ? `: ${capability.reason}` : ""
        }`,
      }),
    );
  }

  const compiledResult = await tryExecutionStepAsync("compile scan provider fragment", () =>
    Promise.resolve(provider.compile(providerRel, context.context)).then(
      unwrapProviderOperationResult,
    ),
  );
  if (Result.isError(compiledResult)) {
    return compiledResult;
  }
  const rowsResult = await tryExecutionStepAsync("execute scan provider fragment", () =>
    Promise.resolve(provider.execute(compiledResult.value, context.context)).then(
      unwrapProviderOperationResult,
    ),
  );
  if (Result.isError(rowsResult)) {
    return rowsResult;
  }
  const projectedResult = tryExecutionStep("map provider rows to logical rows", () =>
    mapProviderRowsToLogical(
      rowsResult.value as QueryRow[],
      scan.select,
      physicalBinding,
      tableDefinition,
      {
        enforceNotNull:
          !context.constraintValidation || context.constraintValidation.mode === "off",
        enforceEnum: !context.constraintValidation || context.constraintValidation.mode === "off",
      },
    ),
  );
  if (Result.isError(projectedResult)) {
    return projectedResult;
  }
  const validatedResult = tryExecutionStep("validate scan result constraints", () => {
    validateTableConstraintRows({
      schema: context.schema,
      tableName: scan.table,
      rows: projectedResult.value,
      ...(context.constraintValidation ? { options: context.constraintValidation } : {}),
    });
  });
  if (Result.isError(validatedResult)) {
    return validatedResult;
  }

  const alias = scan.alias ?? scan.table;
  return Result.ok(projectedResult.value.map((row) => prefixRow(row, alias)));
}

export function executeCteRefResult<TContext>(
  cteRef: Extract<import("@tupl/foundation").RelNode, { kind: "cte_ref" }>,
  context: RelExecutionContext<TContext>,
) {
  const cteRows = context.cteRows.get(cteRef.name);
  if (!cteRows) {
    return Result.err(
      new TuplExecutionError({
        operation: "execute cte ref",
        message: `Missing materialized CTE rows for ${cteRef.name}.`,
      }),
    );
  }

  const scannedRows = scanLocalRows(cteRows, {
    table: cteRef.name,
    ...(cteRef.alias ? { alias: cteRef.alias } : {}),
    select: cteRef.select,
    ...(cteRef.where ? { where: cteRef.where } : {}),
    ...(cteRef.orderBy ? { orderBy: cteRef.orderBy } : {}),
    ...(cteRef.limit != null ? { limit: cteRef.limit } : {}),
    ...(cteRef.offset != null ? { offset: cteRef.offset } : {}),
  });

  const alias = cteRef.alias ?? cteRef.name;
  return Result.ok(scannedRows.map((row) => prefixRow(row, alias)));
}

function mapLogicalColumnsToSource(
  columns: string[],
  binding: NormalizedPhysicalTableBinding | null,
): string[] {
  if (!binding) {
    return columns;
  }
  return columns.map((column) => resolveNormalizedColumnSource(binding, column));
}

function mapWhereToSource(
  where: ScanFilterClause[],
  binding: NormalizedPhysicalTableBinding | null,
): ScanFilterClause[] {
  if (!binding) {
    return where;
  }

  return where.map((clause) => ({
    ...clause,
    column: resolveNormalizedColumnSource(binding, clause.column),
  }));
}

function mapOrderToSource(
  orderBy: NonNullable<TableScanRequest["orderBy"]>,
  binding: NormalizedPhysicalTableBinding | null,
): NonNullable<TableScanRequest["orderBy"]> {
  if (!binding) {
    return orderBy;
  }

  return orderBy.map((term) => ({
    ...term,
    column: resolveNormalizedColumnSource(binding, term.column),
  }));
}
