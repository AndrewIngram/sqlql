/**
 * Timestamp helpers own schema-facing timestamp value normalization.
 */
declare const ISO_8601_TIMESTAMP_BRAND: unique symbol;

export type Iso8601TimestampString = string & {
  readonly [ISO_8601_TIMESTAMP_BRAND]: "Iso8601TimestampString";
};

export type TimestampValue = Iso8601TimestampString | string | Date;

export function asIso8601Timestamp(value: string | Date): Iso8601TimestampString {
  return (value instanceof Date ? value.toISOString() : value) as Iso8601TimestampString;
}
