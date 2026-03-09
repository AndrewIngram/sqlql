import { createDataEntityHandle } from "@tupl/core";
import { createSchemaBuilder } from "@tupl/core/schema";

const ordersEntity = createDataEntityHandle({
  entity: "orders_raw",
  provider: "warehouse",
});
const vendorsEntity = createDataEntityHandle<"id" | "name">({
  entity: "vendors_raw",
  provider: "warehouse",
});
const typedMetricsEntity = createDataEntityHandle<
  | "id"
  | "integerColumn"
  | "realColumn"
  | "timestampColumn"
  | "jsonColumn"
  | "blobColumn"
  | "dateColumn"
  | "datetimeColumn",
  {
    id: string;
    integerColumn: number;
    realColumn: number;
    timestampColumn: Date;
    jsonColumn: unknown;
    blobColumn: Uint8Array | null;
    dateColumn: string | null;
    datetimeColumn: string | null;
  },
  {
    id: { source: "id"; type: "text"; nullable: false; primaryKey: true };
    integerColumn: { source: "integer_column"; type: "integer"; nullable: false };
    realColumn: { source: "real_column"; type: "real"; nullable: false };
    timestampColumn: { source: "timestamp_column"; type: "timestamp"; nullable: false };
    jsonColumn: { source: "json_column"; type: "json"; nullable: true };
    blobColumn: { source: "blob_column"; type: "blob"; nullable: true };
    dateColumn: { source: "date_column"; type: "date"; nullable: true };
    datetimeColumn: { source: "datetime_column"; type: "datetime"; nullable: true };
  }
>({
  entity: "typed_metrics_raw",
  provider: "warehouse",
  columns: {
    id: { source: "id", type: "text", nullable: false, primaryKey: true },
    integerColumn: { source: "integer_column", type: "integer", nullable: false },
    realColumn: { source: "real_column", type: "real", nullable: false },
    timestampColumn: { source: "timestamp_column", type: "timestamp", nullable: false },
    jsonColumn: { source: "json_column", type: "json", nullable: true },
    blobColumn: { source: "blob_column", type: "blob", nullable: true },
    dateColumn: { source: "date_column", type: "date", nullable: true },
    datetimeColumn: { source: "datetime_column", type: "datetime", nullable: true },
  },
});

const schemaBuilder = createSchemaBuilder<Record<string, never>>();

const myOrders = schemaBuilder.table("myOrders", ordersEntity, {
  columns: () => ({
    id: { source: "id", type: "text", nullable: false },
    vendorId: { source: "vendor_id", type: "text" },
    totalCents: { source: "total_cents", type: "integer" },
  }),
});

const typedOrders = schemaBuilder.table("typedOrders", vendorsEntity, {
  columns: ({ col }) => ({
    id: col.id("id"),
    name: col.string("name"),
    // @ts-expect-error - typed builders still validate source columns against the bound entity
    missing: col.string("doesNotExist"),
  }),
});
void typedOrders;

const typedMetrics = schemaBuilder.table("typedMetrics", typedMetricsEntity, {
  columns: ({ col }) => ({
    integerValue: col.integer("integerColumn"),
    realValue: col.real("realColumn"),
    timestampValue: col.timestamp("timestampColumn"),
    jsonValue: col.json("jsonColumn"),
    blobValue: col.blob("blobColumn"),
    dateValue: col.date("dateColumn"),
    datetimeValue: col.datetime("datetimeColumn"),
    coercedInteger: col.integer("timestampColumn", { coerce: "isoTimestamp" }),
    // @ts-expect-error - incompatible source types require explicit coerce
    invalidInteger: col.integer("timestampColumn"),
    // @ts-expect-error - integers are not accepted by text builders without explicit coerce
    invalidString: col.string("integerColumn"),
  }),
});
void typedMetrics;

schemaBuilder.view(
  "spendByVendor",
  ({ scan, join, aggregate, col, expr, agg }) => {
    const entityScan = scan(vendorsEntity);
    void entityScan;

    const okEntityColRef = col(vendorsEntity, "id");
    void okEntityColRef;
    // @ts-expect-error - column does not exist on vendorsEntity
    col(vendorsEntity, "doesNotExist");

    const okColRef = col(myOrders, "vendorId");
    void okColRef;
    // @ts-expect-error - column does not exist on myOrders
    col(myOrders, "doesNotExist");

    return aggregate({
      from: join({
        left: scan(myOrders),
        right: scan(myOrders),
        on: expr.eq(col(myOrders, "vendorId"), col(myOrders, "id")),
        type: "inner",
      }),
      groupBy: {
        vendorId: col(myOrders, "vendorId"),
      },
      measures: {
        spend: agg.sum(col(myOrders, "totalCents")),
      },
    });
  },
  {
    columns: ({ col }) => ({
      vendorId: col.string("vendorId"),
      spend: col.integer("spend"),
    }),
  },
);

schemaBuilder.build();
