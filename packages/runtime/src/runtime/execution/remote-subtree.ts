import { Result } from "better-result";

import { TuplExecutionError, type RelNode } from "@tupl/foundation";
import {
  getDataEntityProvider,
  normalizeCapability,
  unwrapProviderOperationResult,
  type ProviderAdapter,
} from "@tupl/provider-kit";
import { buildProviderFragmentForRelResult } from "@tupl/planner";
import { mapProviderRowsToRelOutput } from "@tupl/schema-model/mapping";

import {
  tryExecutionStep,
  tryExecutionStepAsync,
  type RemoteExecutionResult,
  type RelExecutionContext,
} from "./local-execution";

/**
 * Remote subtree owns provider pushdown for non-scan relational subtrees.
 */
export async function tryExecuteRemoteSubtreeResult<TContext>(
  node: RelNode,
  context: RelExecutionContext<TContext>,
): Promise<RemoteExecutionResult> {
  if (node.kind === "scan") {
    return Result.ok(null);
  }

  const fragmentResult = buildProviderFragmentForRelResult(node, context.schema, context.context);
  if (Result.isError(fragmentResult)) {
    return fragmentResult;
  }
  const fragment = fragmentResult.value;
  if (!fragment) {
    return Result.ok(null);
  }

  const provider = resolveProviderForNode(node, fragment.provider, context);
  if (!provider) {
    return Result.err(
      new TuplExecutionError({
        operation: "execute relational node",
        message: `Missing provider: ${fragment.provider}`,
      }),
    );
  }

  const capabilityResult = await tryExecutionStepAsync("check subtree provider capability", () =>
    Promise.resolve(provider.canExecute(fragment.rel, context.context)),
  );
  if (Result.isError(capabilityResult)) {
    return capabilityResult;
  }

  const capability = normalizeCapability(capabilityResult.value);
  if (!capability.supported) {
    return Result.ok(null);
  }

  const compiledResult = await tryExecutionStepAsync("compile subtree provider fragment", () =>
    Promise.resolve(provider.compile(fragment.rel, context.context)).then(
      unwrapProviderOperationResult,
    ),
  );
  if (Result.isError(compiledResult)) {
    return compiledResult;
  }

  const rowsResult = await tryExecutionStepAsync("execute subtree provider fragment", () =>
    Promise.resolve(provider.execute(compiledResult.value, context.context)).then(
      unwrapProviderOperationResult,
    ),
  );
  if (Result.isError(rowsResult)) {
    return rowsResult;
  }

  return tryExecutionStep("map provider rows to logical rel output rows", () =>
    mapProviderRowsToRelOutput(rowsResult.value, fragment.rel, context.schema),
  );
}

function resolveProviderForNode<TContext>(
  node: RelNode,
  providerName: string,
  context: RelExecutionContext<TContext>,
): ProviderAdapter<TContext> | undefined {
  return context.providers[providerName] ?? findNodeProvider(node, providerName);
}

function findNodeProvider<TContext>(
  node: RelNode,
  providerName: string,
): ProviderAdapter<TContext> | undefined {
  switch (node.kind) {
    case "scan": {
      if (!node.entity || node.entity.provider !== providerName) {
        return undefined;
      }
      return getDataEntityProvider(node.entity) as ProviderAdapter<TContext> | undefined;
    }
    case "filter":
    case "project":
    case "aggregate":
    case "window":
    case "sort":
    case "limit_offset":
      return findNodeProvider(node.input, providerName);
    case "join":
    case "set_op":
      return (
        findNodeProvider(node.left, providerName) ?? findNodeProvider(node.right, providerName)
      );
    case "with":
      return (
        node.ctes.map((cte) => findNodeProvider(cte.query, providerName)).find(Boolean) ??
        findNodeProvider(node.body, providerName)
      );
    case "values":
    case "cte_ref":
    case "correlate":
    case "repeat_union":
      return undefined;
  }
}
