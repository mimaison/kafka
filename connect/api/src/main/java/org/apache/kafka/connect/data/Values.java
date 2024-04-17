/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.data;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.errors.DataException;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Pattern;

/**
 * Utility for converting from one Connect value to a different form. This is useful when the caller expects a value of a particular type
 * but is uncertain whether the actual value is one that isn't directly that type but can be converted into that type.
 *
 * <p>For example, a caller might expect a particular {@link org.apache.kafka.connect.header.Header} to contain a {@link Type#INT64}
 * value, when in fact that header contains a string representation of a 32-bit integer. Here, the caller can use the methods in this
 * class to convert the value to the desired type:
 * <pre>
 *     Header header = ...
 *     long value = Values.convertToLong(header.schema(), header.value());
 * </pre>
 *
 * <p>This class is able to convert any value to a string representation as well as parse those string representations back into most of
 * the types. The only exception is {@link Struct} values that require a schema and thus cannot be parsed from a simple string.
 */
public class Values {

    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    private static final Schema ARRAY_SELECTOR_SCHEMA = SchemaBuilder.array(Schema.STRING_SCHEMA).build();
    private static final Schema MAP_SELECTOR_SCHEMA = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build();
    private static final Schema STRUCT_SELECTOR_SCHEMA = SchemaBuilder.struct().build();

    private static final Pattern TWO_BACKSLASHES = Pattern.compile("\\\\");
    private static final Pattern DOUBLE_QUOTE = Pattern.compile("\"");

    private static final long MILLIS_PER_DAY = 24 * 60 * 60 * 1000;

    public static final String NULL_VALUE = "null";
    public static final String ISO_8601_DATE_FORMAT_PATTERN = "yyyy-MM-dd";
    public static final String ISO_8601_TIME_FORMAT_PATTERN = "HH:mm:ss.SSS'Z'";
    public static final String ISO_8601_TIMESTAMP_FORMAT_PATTERN = ISO_8601_DATE_FORMAT_PATTERN + "'T'" + ISO_8601_TIME_FORMAT_PATTERN;

    /**
     * Convert the specified value to a {@link Type#BOOLEAN} value. The supplied schema is required if the value is a logical
     * type when the schema contains critical information that might be necessary for converting to a boolean.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a boolean, or null if the supplied value was null
     * @throws DataException if the value could not be converted to a boolean
     */
    public static Boolean convertToBoolean(Schema schema, Object value) throws DataException {
        if (value == null) {
            return null;
        } else if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof String) {
            SchemaAndValue parsed = Parser.parseString(value.toString());
            if (parsed.value() instanceof Boolean) {
                return (Boolean) parsed.value();
            }
        }
        return asLong(value, schema, null) == 0L ? Boolean.FALSE : Boolean.TRUE;
    }

    /**
     * Convert the specified value to an {@link Type#INT8} byte value. The supplied schema is required if the value is a logical
     * type when the schema contains critical information that might be necessary for converting to a byte.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a byte, or null if the supplied value was null
     * @throws DataException if the value could not be converted to a byte
     */
    public static Byte convertToByte(Schema schema, Object value) throws DataException {
        if (value == null) {
            return null;
        } else if (value instanceof Byte) {
            return (Byte) value;
        }
        return (byte) asLong(value, schema, null);
    }

    /**
     * Convert the specified value to an {@link Type#INT16} short value. The supplied schema is required if the value is a logical
     * type when the schema contains critical information that might be necessary for converting to a short.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a short, or null if the supplied value was null
     * @throws DataException if the value could not be converted to a short
     */
    public static Short convertToShort(Schema schema, Object value) throws DataException {
        if (value == null) {
            return null;
        } else if (value instanceof Short) {
            return (Short) value;
        }
        return (short) asLong(value, schema, null);
    }

    /**
     * Convert the specified value to an {@link Type#INT32} int value. The supplied schema is required if the value is a logical
     * type when the schema contains critical information that might be necessary for converting to an integer.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as an integer, or null if the supplied value was null
     * @throws DataException if the value could not be converted to an integer
     */
    public static Integer convertToInteger(Schema schema, Object value) throws DataException {
        if (value == null) {
            return null;
        } else if (value instanceof Integer) {
            return (Integer) value;
        }
        return (int) asLong(value, schema, null);
    }

    /**
     * Convert the specified value to an {@link Type#INT64} long value. The supplied schema is required if the value is a logical
     * type when the schema contains critical information that might be necessary for converting to a long.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a long, or null if the supplied value was null
     * @throws DataException if the value could not be converted to a long
     */
    public static Long convertToLong(Schema schema, Object value) throws DataException {
        if (value == null) {
            return null;
        } else if (value instanceof Long) {
            return (Long) value;
        }
        return asLong(value, schema, null);
    }

    /**
     * Convert the specified value to a {@link Type#FLOAT32} float value. The supplied schema is required if the value is a logical
     * type when the schema contains critical information that might be necessary for converting to a floating point number.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a float, or null if the supplied value was null
     * @throws DataException if the value could not be converted to a float
     */
    public static Float convertToFloat(Schema schema, Object value) throws DataException {
        if (value == null) {
            return null;
        } else if (value instanceof Float) {
            return (Float) value;
        }
        return (float) asDouble(value, schema, null);
    }

    /**
     * Convert the specified value to a {@link Type#FLOAT64} double value. The supplied schema is required if the value is a logical
     * type when the schema contains critical information that might be necessary for converting to a floating point number.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a double, or null if the supplied value was null
     * @throws DataException if the value could not be converted to a double
     */
    public static Double convertToDouble(Schema schema, Object value) throws DataException {
        if (value == null) {
            return null;
        } else if (value instanceof Double) {
            return (Double) value;
        }
        return asDouble(value, schema, null);
    }

    /**
     * Convert the specified value to a {@link Type#STRING} value.
     * Not supplying a schema may limit the ability to convert to the desired type.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a string, or null if the supplied value was null
     */
    public static String convertToString(Schema schema, Object value) {
        if (value == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        append(sb, value, false);
        return sb.toString();
    }

    /**
     * Convert the specified value to an {@link Type#ARRAY} value. If the value is a string representation of an array, this method
     * will parse the string and its elements to infer the schemas for those elements. Thus, this method supports
     * arrays of other primitives and structured types. If the value is already an array (or list), this method simply casts and
     * returns it.
     *
     * <p>This method currently does not use the schema, though it may be used in the future.</p>
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a list, or null if the supplied value was null
     * @throws DataException if the value cannot be converted to a list value
     */
    public static List<?> convertToList(Schema schema, Object value) {
        return convertToArray(ARRAY_SELECTOR_SCHEMA, value);
    }

    /**
     * Convert the specified value to a {@link Type#MAP} value. If the value is a string representation of a map, this method
     * will parse the string and its entries to infer the schemas for those entries. Thus, this method supports
     * maps with primitives and structured keys and values. If the value is already a map, this method simply casts and returns it.
     *
     * <p>This method currently does not use the schema, though it may be used in the future.</p>
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a map, or null if the supplied value was null
     * @throws DataException if the value cannot be converted to a map value
     */
    public static Map<?, ?> convertToMap(Schema schema, Object value) {
        return convertToMapInternal(MAP_SELECTOR_SCHEMA, value);
    }

    /**
     * Convert the specified value to a {@link Type#STRUCT} value. Structs cannot be converted from other types, so this method returns
     * a struct only if the supplied value is a struct. If not a struct, this method throws an exception.
     *
     * <p>This method currently does not use the schema, though it may be used in the future.</p>
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a struct, or null if the supplied value was null
     * @throws DataException if the value is not a struct
     */
    public static Struct convertToStruct(Schema schema, Object value) {
        return convertToStructInternal(STRUCT_SELECTOR_SCHEMA, value);
    }

    /**
     * Convert the specified value to a {@link Time#SCHEMA time} value.
     * Not supplying a schema may limit the ability to convert to the desired type.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a time, or null if the supplied value was null
     * @throws DataException if the value cannot be converted to a time value
     */
    public static java.util.Date convertToTime(Schema schema, Object value) {
        if (value == null) {
            throw new DataException("Unable to convert a null value to a schema that requires a value");
        }
        return convertToTime(Time.SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to a {@link Date#SCHEMA date} value.
     * Not supplying a schema may limit the ability to convert to the desired type.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a date, or null if the supplied value was null
     * @throws DataException if the value cannot be converted to a date value
     */
    public static java.util.Date convertToDate(Schema schema, Object value) {
        if (value == null) {
            throw new DataException("Unable to convert a null value to a schema that requires a value");
        }
        return convertToDate(Date.SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to a {@link Timestamp#SCHEMA timestamp} value.
     * Not supplying a schema may limit the ability to convert to the desired type.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a timestamp, or null if the supplied value was null
     * @throws DataException if the value cannot be converted to a timestamp value
     */
    public static java.util.Date convertToTimestamp(Schema schema, Object value) {
        if (value == null) {
            throw new DataException("Unable to convert a null value to a schema that requires a value");
        }
        return convertToTimestamp(Timestamp.SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to a {@link Decimal decimal} value.
     * Not supplying a schema may limit the ability to convert to the desired type.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a decimal, or null if the supplied value was null
     * @throws DataException if the value cannot be converted to a decimal value
     */
    public static BigDecimal convertToDecimal(Schema schema, Object value, int scale) {
        if (value == null) {
            throw new DataException("Unable to convert a null value to a schema that requires a value");
        }
        return convertToDecimal(Decimal.schema(scale), value);
    }

    /**
     * If possible infer a schema for the given value.
     *
     * @param value the value whose schema is to be inferred; may be null
     * @return the inferred schema, or null if the value is null or no schema could be inferred
     */
    public static Schema inferSchema(Object value) {
        if (value instanceof String) {
            return Schema.STRING_SCHEMA;
        } else if (value instanceof Boolean) {
            return Schema.BOOLEAN_SCHEMA;
        } else if (value instanceof Byte) {
            return Schema.INT8_SCHEMA;
        } else if (value instanceof Short) {
            return Schema.INT16_SCHEMA;
        } else if (value instanceof Integer) {
            return Schema.INT32_SCHEMA;
        } else if (value instanceof Long) {
            return Schema.INT64_SCHEMA;
        } else if (value instanceof Float) {
            return Schema.FLOAT32_SCHEMA;
        } else if (value instanceof Double) {
            return Schema.FLOAT64_SCHEMA;
        } else if (value instanceof byte[] || value instanceof ByteBuffer) {
            return Schema.BYTES_SCHEMA;
        } else if (value instanceof List) {
            return inferListSchema((List<?>) value);
        } else if (value instanceof Map) {
            return inferMapSchema((Map<?, ?>) value);
        } else if (value instanceof Struct) {
            return ((Struct) value).schema();
        }
        return null;
    }

    private static Schema inferListSchema(List<?> list) {
        if (list.isEmpty()) {
            return null;
        }
        SchemaDetector detector = new SchemaDetector();
        for (Object element : list) {
            if (!detector.canDetect(element)) {
                return null;
            }
        }
        return SchemaBuilder.array(detector.schema()).build();
    }

    private static Schema inferMapSchema(Map<?, ?> map) {
        if (map.isEmpty()) {
            return null;
        }
        SchemaDetector keyDetector = new SchemaDetector();
        SchemaDetector valueDetector = new SchemaDetector();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!keyDetector.canDetect(entry.getKey()) || !valueDetector.canDetect(entry.getValue())) {
                return null;
            }
        }
        return SchemaBuilder.map(keyDetector.schema(), valueDetector.schema()).build();
    }

    /**
     * Convert the value to the desired type.
     *
     * @param toSchema   the schema for the desired type; may not be null
     * @param fromSchema the schema for the supplied value; may be null if not known
     * @return the converted value; null if the passed-in schema was optional, and the input value was null.
     * @throws DataException if the value could not be converted to the desired type
     */
    protected static Object convertTo(Schema toSchema, Schema fromSchema, Object value) throws DataException {
        if (value == null) {
            if (toSchema.isOptional()) {
                return null;
            }
            throw new DataException("Unable to convert a null value to a schema that requires a value");
        }
        switch (toSchema.type()) {
            case BYTES:
                return convertMaybeLogicalBytes(toSchema, value);
            case STRING:
                return convertToString(fromSchema, value);
            case BOOLEAN:
                return convertToBoolean(fromSchema, value);
            case INT8:
                return convertToByte(fromSchema, value);
            case INT16:
                return convertToShort(fromSchema, value);
            case INT32:
                return convertMaybeLogicalInteger(toSchema, fromSchema, value);
            case INT64:
                return convertMaybeLogicalLong(toSchema, fromSchema, value);
            case FLOAT32:
                return convertToFloat(fromSchema, value);
            case FLOAT64:
                return convertToDouble(fromSchema, value);
            case ARRAY:
                return convertToArray(toSchema, value);
            case MAP:
                return convertToMapInternal(toSchema, value);
            case STRUCT:
                return convertToStructInternal(toSchema, value);
        }
        throw new DataException("Unable to convert " + value + " (" + value.getClass() + ") to " + toSchema);
    }

    private static Serializable convertMaybeLogicalBytes(Schema toSchema, Object value) {
        if (Decimal.LOGICAL_NAME.equals(toSchema.name())) {
            return convertToDecimal(toSchema, value);
        }
        return convertToBytes(toSchema, value);
    }

    private static BigDecimal convertToDecimal(Schema toSchema, Object value) {
        if (value instanceof ByteBuffer) {
            value = Utils.toArray((ByteBuffer) value);
        }
        if (value instanceof byte[]) {
            return Decimal.toLogical(toSchema, (byte[]) value);
        }
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof Number) {
            // Not already a decimal, so treat it as a double ...
            double converted = ((Number) value).doubleValue();
            return BigDecimal.valueOf(converted);
        }
        if (value instanceof String) {
            return new BigDecimal(value.toString());
        }
        throw new DataException("Unable to convert " + value + " (" + value.getClass() + ") to " + toSchema);
    }

    private static byte[] convertToBytes(Schema toSchema, Object value) {
        if (value instanceof ByteBuffer) {
            return Utils.toArray((ByteBuffer) value);
        }
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        if (value instanceof BigDecimal) {
            return Decimal.fromLogical(toSchema, (BigDecimal) value);
        }
        throw new DataException("Unable to convert " + value + " (" + value.getClass() + ") to " + toSchema);
    }

    private static Serializable convertMaybeLogicalInteger(Schema toSchema, Schema fromSchema, Object value) {
        if (Date.LOGICAL_NAME.equals(toSchema.name())) {
            return convertToDate(toSchema, fromSchema, value);
        }
        if (Time.LOGICAL_NAME.equals(toSchema.name())) {
            return convertToTime(toSchema, fromSchema, value);
        }
        return convertToInteger(fromSchema, value);
    }

    private static java.util.Date convertToDate(Schema toSchema, Schema fromSchema, Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof String) {
            SchemaAndValue parsed = Parser.parseString(value.toString());
            value = parsed.value();
        }
        if (value instanceof java.util.Date) {
            if (fromSchema != null) {
                String fromSchemaName = fromSchema.name();
                if (Date.LOGICAL_NAME.equals(fromSchemaName)) {
                    return (java.util.Date) value;
                }
                if (Timestamp.LOGICAL_NAME.equals(fromSchemaName)) {
                    // Just get the number of days from this timestamp
                    long millis = ((java.util.Date) value).getTime();
                    int days = (int) (millis / MILLIS_PER_DAY); // truncates
                    return Date.toLogical(toSchema, days);
                }
            } else {
                // There is no fromSchema, so no conversion is needed
                return (java.util.Date) value;
            }
        }
        long numeric = asLong(value, fromSchema, null);
        return Date.toLogical(toSchema, (int) numeric);
    }

    private static java.util.Date convertToTime(Schema toSchema, Schema fromSchema, Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof String) {
            SchemaAndValue parsed = Parser.parseString(value.toString());
            value = parsed.value();
        }
        if (value instanceof java.util.Date) {
            if (fromSchema != null) {
                String fromSchemaName = fromSchema.name();
                if (Time.LOGICAL_NAME.equals(fromSchemaName)) {
                    return (java.util.Date) value;
                }
                if (Timestamp.LOGICAL_NAME.equals(fromSchemaName)) {
                    // Just get the time portion of this timestamp
                    Calendar calendar = Calendar.getInstance(UTC);
                    calendar.setTime((java.util.Date) value);
                    calendar.set(Calendar.YEAR, 1970);
                    calendar.set(Calendar.MONTH, 0); // Months are zero-based
                    calendar.set(Calendar.DAY_OF_MONTH, 1);
                    return Time.toLogical(toSchema, (int) calendar.getTimeInMillis());
                }
            } else {
                // There is no fromSchema, so no conversion is needed
                return (java.util.Date) value;
            }
        }
        long numeric = asLong(value, fromSchema, null);
        return Time.toLogical(toSchema, (int) numeric);
    }

    private static Serializable convertMaybeLogicalLong(Schema toSchema, Schema fromSchema, Object value) {
        if (Timestamp.LOGICAL_NAME.equals(toSchema.name())) {
            return convertToTimestamp(toSchema, fromSchema, value);
        }
        return convertToLong(fromSchema, value);
    }

    private static java.util.Date convertToTimestamp(Schema toSchema, Schema fromSchema, Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof String) {
            SchemaAndValue parsed = Parser.parseString(value.toString());
            value = parsed.value();
        }
        if (value instanceof java.util.Date) {
            java.util.Date date = (java.util.Date) value;
            if (fromSchema != null) {
                String fromSchemaName = fromSchema.name();
                if (Date.LOGICAL_NAME.equals(fromSchemaName)) {
                    int days = Date.fromLogical(fromSchema, date);
                    long millis = days * MILLIS_PER_DAY;
                    return Timestamp.toLogical(toSchema, millis);
                }
                if (Time.LOGICAL_NAME.equals(fromSchemaName)) {
                    long millis = Time.fromLogical(fromSchema, date);
                    return Timestamp.toLogical(toSchema, millis);
                }
                if (Timestamp.LOGICAL_NAME.equals(fromSchemaName)) {
                    return date;
                }
            } else {
                // There is no fromSchema, so no conversion is needed
                return date;
            }
        }
        long numeric = asLong(value, fromSchema, null);
        return Timestamp.toLogical(toSchema, numeric);
    }

    private static List<?> convertToArray(Schema toSchema, Object value) {
        if (value == null) {
            throw new DataException("Unable to convert a null value to a schema that requires a value");
        } else if (value instanceof String) {
            SchemaAndValue schemaAndValue = Parser.parseString(value.toString());
            value = schemaAndValue.value();
        }
        if (value instanceof List) {
            return (List<?>) value;
        }
        throw new DataException("Unable to convert " + value + " (" + value.getClass() + ") to " + toSchema);
    }

    private static Map<?, ?> convertToMapInternal(Schema toSchema, Object value) {
        if (value == null) {
            throw new DataException("Unable to convert a null value to a schema that requires a value");
        } else if (value instanceof String) {
            SchemaAndValue schemaAndValue = Parser.parseString(value.toString());
            value = schemaAndValue.value();
        }
        if (value instanceof Map) {
            return (Map<?, ?>) value;
        }
        throw new DataException("Unable to convert " + value + " (" + value.getClass() + ") to " + toSchema);
    }

    private static Struct convertToStructInternal(Schema toSchema, Object value) {
        if (value == null) {
            throw new DataException("Unable to convert a null value to a schema that requires a value");
        } else if (value instanceof Struct) {
            return (Struct) value;
        }
        throw new DataException("Unable to convert " + value + " (" + value.getClass() + ") to " + toSchema);
    }

    /**
     * Convert the specified value to the desired scalar value type.
     *
     * @param value      the value to be converted; may not be null
     * @param fromSchema the schema for the current value type; may not be null
     * @param error      any previous error that should be included in an exception message; may be null
     * @return the long value after conversion; never null
     * @throws DataException if the value could not be converted to a long
     */
    protected static long asLong(Object value, Schema fromSchema, Throwable error) {
        try {
            if (value instanceof Number) {
                Number number = (Number) value;
                return number.longValue();
            }
            if (value instanceof String) {
                return new BigDecimal(value.toString()).longValue();
            }
        } catch (NumberFormatException e) {
            error = e;
            // fall through
        }
        if (fromSchema != null) {
            String schemaName = fromSchema.name();
            if (value instanceof java.util.Date) {
                if (Date.LOGICAL_NAME.equals(schemaName)) {
                    return Date.fromLogical(fromSchema, (java.util.Date) value);
                }
                if (Time.LOGICAL_NAME.equals(schemaName)) {
                    return Time.fromLogical(fromSchema, (java.util.Date) value);
                }
                if (Timestamp.LOGICAL_NAME.equals(schemaName)) {
                    return Timestamp.fromLogical(fromSchema, (java.util.Date) value);
                }
            }
            throw new DataException("Unable to convert " + value + " (" + value.getClass() + ") to " + fromSchema, error);
        }
        throw new DataException("Unable to convert " + value + " (" + value.getClass() + ") to a number", error);
    }

    /**
     * Convert the specified value with the desired floating point type.
     *
     * @param value  the value to be converted; may not be null
     * @param schema the schema for the current value type; may not be null
     * @param error  any previous error that should be included in an exception message; may be null
     * @return the double value after conversion; never null
     * @throws DataException if the value could not be converted to a double
     */
    protected static double asDouble(Object value, Schema schema, Throwable error) {
        try {
            if (value instanceof Number) {
                Number number = (Number) value;
                return number.doubleValue();
            }
            if (value instanceof String) {
                return new BigDecimal(value.toString()).doubleValue();
            }
        } catch (NumberFormatException e) {
            error = e;
            // fall through
        }
        return asLong(value, schema, error);
    }

    protected static void append(StringBuilder sb, Object value, boolean embedded) {
        if (value == null) {
            sb.append(NULL_VALUE);
        } else if (value instanceof Number) {
            sb.append(value);
        } else if (value instanceof Boolean) {
            sb.append(value);
        } else if (value instanceof String) {
            if (embedded) {
                String escaped = escape((String) value);
                sb.append('"').append(escaped).append('"');
            } else {
                sb.append(value);
            }
        } else if (value instanceof byte[]) {
            value = Base64.getEncoder().encodeToString((byte[]) value);
            if (embedded) {
                sb.append('"').append(value).append('"');
            } else {
                sb.append(value);
            }
        } else if (value instanceof ByteBuffer) {
            byte[] bytes = Utils.readBytes((ByteBuffer) value);
            append(sb, bytes, embedded);
        } else if (value instanceof List) {
            List<?> list = (List<?>) value;
            sb.append('[');
            appendIterable(sb, list.iterator());
            sb.append(']');
        } else if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            sb.append('{');
            appendIterable(sb, map.entrySet().iterator());
            sb.append('}');
        } else if (value instanceof Struct) {
            Struct struct = (Struct) value;
            Schema schema = struct.schema();
            boolean first = true;
            sb.append('{');
            for (Field field : schema.fields()) {
                if (first) {
                    first = false;
                } else {
                    sb.append(',');
                }
                append(sb, field.name(), true);
                sb.append(':');
                append(sb, struct.get(field), true);
            }
            sb.append('}');
        } else if (value instanceof Map.Entry) {
            Map.Entry<?, ?> entry = (Map.Entry<?, ?>) value;
            append(sb, entry.getKey(), true);
            sb.append(':');
            append(sb, entry.getValue(), true);
        } else if (value instanceof java.util.Date) {
            java.util.Date dateValue = (java.util.Date) value;
            String formatted = dateFormatFor(dateValue).format(dateValue);
            sb.append(formatted);
        } else {
            throw new DataException("Failed to serialize unexpected value type " + value.getClass().getName() + ": " + value);
        }
    }

    protected static void appendIterable(StringBuilder sb, Iterator<?> iter) {
        if (iter.hasNext()) {
            append(sb, iter.next(), true);
            while (iter.hasNext()) {
                sb.append(',');
                append(sb, iter.next(), true);
            }
        }
    }

    protected static String escape(String value) {
        String replace1 = TWO_BACKSLASHES.matcher(value).replaceAll("\\\\\\\\");
        return DOUBLE_QUOTE.matcher(replace1).replaceAll("\\\\\"");
    }

    public static DateFormat dateFormatFor(java.util.Date value) {
        if (value.getTime() < MILLIS_PER_DAY) {
            return new SimpleDateFormat(ISO_8601_TIME_FORMAT_PATTERN);
        }
        if (value.getTime() % MILLIS_PER_DAY == 0) {
            return new SimpleDateFormat(ISO_8601_DATE_FORMAT_PATTERN);
        }
        return new SimpleDateFormat(ISO_8601_TIMESTAMP_FORMAT_PATTERN);
    }

    protected static class SchemaDetector {
        private Type knownType = null;
        private boolean optional = false;

        public SchemaDetector() {
        }

        public boolean canDetect(Object value) {
            if (value == null) {
                optional = true;
                return true;
            }
            Schema schema = inferSchema(value);
            if (schema == null) {
                return false;
            }
            if (knownType == null) {
                knownType = schema.type();
            } else if (knownType != schema.type()) {
                return false;
            }
            return true;
        }

        public Schema schema() {
            SchemaBuilder builder = SchemaBuilder.type(knownType);
            if (optional) {
                builder.optional();
            }
            return builder.schema();
        }
    }


}
