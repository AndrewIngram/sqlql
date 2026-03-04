import {
  defineProviders,
  type ProviderAdapter,
  type ProviderFragment,
  type ProvidersMap,
  type QueryRow,
  type TableMethodsMap,
} from "../../src";

export function providersFromMethods<TContext>(
  methods: TableMethodsMap<TContext>,
  providerName = "legacy",
): ProvidersMap<TContext> {
  const adapter: ProviderAdapter<TContext> = {
    canExecute(fragment) {
      switch (fragment.kind) {
        case "scan":
          return !!methods[fragment.table]?.scan;
        case "aggregate":
          return !!methods[fragment.table]?.aggregate;
        case "rel":
          return false;
        default:
          return false;
      }
    },
    async compile(fragment) {
      return {
        provider: providerName,
        kind: fragment.kind,
        payload: fragment,
      };
    },
    async execute(plan, context) {
      const fragment = plan.payload as ProviderFragment;
      switch (fragment.kind) {
        case "scan": {
          const method = methods[fragment.table];
          if (!method?.scan) {
            throw new Error(`No table methods registered for table: ${fragment.table}`);
          }
          return method.scan(fragment.request, context);
        }
        case "aggregate": {
          const method = methods[fragment.table];
          if (!method?.aggregate) {
            throw new Error(`No aggregate method registered for table: ${fragment.table}`);
          }
          return method.aggregate(fragment.request, context);
        }
        case "rel":
          throw new Error("Methods-based provider does not support rel fragments.");
      }
    },
    async lookupMany(request, context): Promise<QueryRow[]> {
      const method = methods[request.table];
      if (!method?.lookup) {
        return [];
      }

      return method.lookup(
        {
          table: request.table,
          key: request.key,
          values: request.keys,
          select: request.select,
          ...(request.where ? { where: request.where } : {}),
        },
        context,
      );
    },
  };

  return defineProviders({
    [providerName]: adapter,
  });
}
