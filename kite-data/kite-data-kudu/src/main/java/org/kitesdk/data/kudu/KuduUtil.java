/*
 * Copyright 2015 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.kudu;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.DiscreteDomains;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.util.Utf8;
import org.kitesdk.data.spi.Conversions;
import org.kitesdk.data.spi.EntityAccessor;
import org.kitesdk.data.spi.predicates.Range;
import org.kitesdk.data.spi.predicates.Ranges;
import org.kududb.ColumnSchema;
import org.kududb.client.ColumnRangePredicate;
import org.kududb.client.Insert;
import org.kududb.client.KuduTable;
import org.kududb.client.PartialRow;
import org.kududb.client.RowResult;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class KuduUtil {

  public static <E> Insert buildInsert(E entity, KuduTable table,
                                       EntityAccessor<E> accessor) {
    Insert insert = table.newInsert();
    PartialRow row = insert.getRow();
    List<ColumnSchema> columns = table.getSchema().getColumns();
    for (int ordinal = 0; ordinal < columns.size(); ordinal += 1) {
      ColumnSchema column = columns.get(ordinal);
      Object value = accessor.get(entity, column.getName());
      addValue(row, column, ordinal, value);
    }

    return insert;
  }

  public static ColumnRangePredicate toKuduPredicate(Range<?> range,
                                                     ColumnSchema column) {
    Range<?> closed = addMissingEndpoints(column, range);
    ColumnRangePredicate predicate = new ColumnRangePredicate(column);
    switch (column.getType()) {
      case INT8:
        predicate.setLowerBound(((Number) closed.lowerEndpoint()).byteValue());
        predicate.setUpperBound(((Number) closed.upperEndpoint()).byteValue());
        break;
      case INT16:
        predicate.setLowerBound(((Number) closed.lowerEndpoint()).shortValue());
        predicate.setUpperBound(((Number) closed.upperEndpoint()).shortValue());
        break;
      case INT32:
        predicate.setLowerBound(((Number) closed.lowerEndpoint()).intValue());
        predicate.setUpperBound(((Number) closed.upperEndpoint()).intValue());
        break;
      case INT64:
        predicate.setLowerBound(((Number) closed.lowerEndpoint()).longValue());
        predicate.setUpperBound(((Number) closed.upperEndpoint()).longValue());
        break;
      case FLOAT:
        predicate.setLowerBound(((Number) closed.lowerEndpoint()).floatValue());
        predicate.setUpperBound(((Number) closed.upperEndpoint()).floatValue());
        break;
      case DOUBLE:
        predicate.setLowerBound(((Number) closed.lowerEndpoint()).doubleValue());
        predicate.setUpperBound(((Number) closed.upperEndpoint()).doubleValue());
        break;
      case STRING:
        predicate.setLowerBound(closed.lowerEndpoint().toString());
        predicate.setUpperBound(closed.upperEndpoint().toString());
        break;
      case BINARY:
        predicate.setLowerBound(
            copyFromByteBuffer((ByteBuffer) closed.lowerEndpoint()));
        predicate.setUpperBound(
            copyFromByteBuffer((ByteBuffer) closed.upperEndpoint()));
        break;
      default:
        throw new UnsupportedOperationException(String.format(
            "Cannot create a column range predicate for %s: %s",
            column.getName(), column.getType()));
    }
    return predicate;
  }

  public static void addValue(PartialRow row, ColumnSchema column, int ordinal,
                              Object value) {
    switch (column.getType()) {
      case BOOL:
        row.addBoolean(ordinal, Conversions.makeBoolean(value));
        break;
      case INT8:
        row.addByte(ordinal, Conversions.makeInteger(value).byteValue());
        break;
      case INT16:
        row.addShort(ordinal, Conversions.makeInteger(value).shortValue());
        break;
      case INT32:
        row.addInt(ordinal, Conversions.makeInteger(value));
        break;
      case INT64:
        row.addLong(ordinal, Conversions.makeLong(value));
        break;
      case FLOAT:
        row.addFloat(ordinal, Conversions.makeFloat(value));
        break;
      case DOUBLE:
        row.addDouble(ordinal, Conversions.makeDouble(value));
        break;
      case STRING:
        if (value instanceof String) {
          row.addString(ordinal, (String) value);
        } else if (value instanceof Utf8) {
          row.addStringUtf8(ordinal, Arrays.copyOf(((Utf8) value).getBytes(),
              ((Utf8) value).getByteLength()));
        } else if (value instanceof byte[]) {
          row.addStringUtf8(ordinal, Arrays.copyOf(
              (byte[]) value, ((byte[]) value).length));
        } else {
          row.addString(ordinal, Conversions.makeString(value));
        }
        break;
      case BINARY:
        // need to defensively copy binary data
        if (value instanceof ByteBuffer) {
          row.addBinary(ordinal, copyFromByteBuffer((ByteBuffer) value));
        } else if (value instanceof byte[]) {
          row.addBinary(ordinal, Arrays.copyOf(
              (byte[]) value, ((byte[]) value).length));
        } else if (value instanceof String) {
          row.addBinary(ordinal,
              ((String) value).getBytes(StandardCharsets.UTF_8));
        } else if (value instanceof Utf8) {
          row.addBinary(ordinal, Arrays.copyOf(
              ((Utf8) value).getBytes(),
              ((Utf8) value).getByteLength()));
        }
        break;
    }
  }

  public static Object toExclusiveValue(ColumnSchema column, Object value) {
    // TODO: what happens when this overflows integer types?
    switch (column.getType()) {
      case INT8:
        return DiscreteDomains.integers()
            .next(((Number) value).intValue())
            .byteValue();
      case INT16:
        return DiscreteDomains.integers()
            .next(((Number) value).intValue())
            .shortValue();
      case INT32:
        return DiscreteDomains.integers().next(((Number) value).intValue());
      case INT64:
        return DiscreteDomains.longs().next(((Number) value).longValue());
      case STRING:
      case BINARY:
        // need to defensively copy binary data
        if (value instanceof String) {
          return ((String) value) + '\0';
        } else if (value instanceof Utf8) {
          Utf8 utf8 = (Utf8) value;
          int length = utf8.getByteLength();
          byte[] bytes = new byte[length + 1];
          System.arraycopy(utf8.getBytes(), 0, bytes, 0, length);
          bytes[length] = '\0';
          return bytes;
        } else if (value instanceof ByteBuffer) {
          ByteBuffer buf = (ByteBuffer) value;
          byte[] bytes = new byte[buf.remaining() + 1];
          buf.mark();
          buf.get(bytes).reset();
          return bytes;
        } else if (value instanceof byte[]) {
          byte[] original = (byte[]) value;
          byte[] bytes = new byte[original.length + 1];
          System.arraycopy(original, 0, bytes, 0, original.length);
          bytes[original.length] = '\0';
          return bytes;
        } else {
          return Conversions.makeString(value) + '\0';
        }
      default:
        throw new UnsupportedOperationException(String.format(
            "Column %s: %s cannot be used in key columns",
            column.getName(), column.getType()));
    }
  }

  private static final byte[] MIN_BYTES = new byte[0];
  private static final byte[] MAX_BYTES = new byte[] {
      (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
      (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
      (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
      (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
      (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
      (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
      (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
      (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};

  // 0xFFFD is the unicode "replacement character"
  // See http://unicode.org/faq/utf_bom.html
  private static final Utf8 MIN_UTF8 = new Utf8(new byte[0]);
  private static final Utf8 MAX_UTF8 = new Utf8(new byte[] {
      (byte) 0xFF, (byte) 0xFD, (byte) 0xFF, (byte) 0xFD, (byte) 0xFF, (byte) 0xFD, (byte) 0xFF, (byte) 0xFD,
      (byte) 0xFF, (byte) 0xFD, (byte) 0xFF, (byte) 0xFD, (byte) 0xFF, (byte) 0xFD, (byte) 0xFF, (byte) 0xFD,
      (byte) 0xFF, (byte) 0xFD, (byte) 0xFF, (byte) 0xFD, (byte) 0xFF, (byte) 0xFD, (byte) 0xFF, (byte) 0xFD,
      (byte) 0xFF, (byte) 0xFD, (byte) 0xFF, (byte) 0xFD, (byte) 0xFF, (byte) 0xFD, (byte) 0xFF, (byte) 0xFD,
      (byte) 0xFF, (byte) 0xFD, (byte) 0xFF, (byte) 0xFD, (byte) 0xFF, (byte) 0xFD, (byte) 0xFF, (byte) 0xFD,
      (byte) 0xFF, (byte) 0xFD, (byte) 0xFF, (byte) 0xFD, (byte) 0xFF, (byte) 0xFD, (byte) 0xFF, (byte) 0xFD,
      (byte) 0xFF, (byte) 0xFD, (byte) 0xFF, (byte) 0xFD, (byte) 0xFF, (byte) 0xFD, (byte) 0xFF, (byte) 0xFD,
      (byte) 0xFF, (byte) 0xFD, (byte) 0xFF, (byte) 0xFD, (byte) 0xFF, (byte) 0xFD, (byte) 0xFF, (byte) 0xFD});

  @SuppressWarnings("unchecked")
  public static <T> Range<T> addMissingEndpoints(
      ColumnSchema column, Range<T> range) {
    if (range.hasLowerBound()) {
      if (range.hasUpperBound()) {
        return range;
      } else {
        return Ranges.closed(range.lowerEndpoint(), (T) getMaxValue(column));
      }
    } else if (range.hasUpperBound()) {
      return Ranges.closed((T) getMinValue(column), range.upperEndpoint());
    } else {
      return (Range<T>) allValues(column);
    }
  }

  private static Object getMinValue(ColumnSchema column) {
    switch (column.getType()) {
      case INT8:
        return Byte.MIN_VALUE;
      case INT16:
        return Short.MIN_VALUE;
      case INT32:
        return Integer.MIN_VALUE;
      case INT64:
        return Long.MIN_VALUE;
      case FLOAT:
        return Float.MIN_VALUE;
      case DOUBLE:
        return Double.MIN_VALUE;
      case STRING:
      case BINARY:
        return MIN_UTF8;
      default:
        throw new UnsupportedOperationException(String.format(
            "No minimum value for %s: %s",
            column.getName(), column.getType()));
    }
  }

  private static Object getMaxValue(ColumnSchema column) {
    switch (column.getType()) {
      case INT8:
        return Byte.MAX_VALUE;
      case INT16:
        return Short.MAX_VALUE;
      case INT32:
        return Integer.MAX_VALUE;
      case INT64:
        return Long.MAX_VALUE;
      case FLOAT:
        return Float.MAX_VALUE;
      case DOUBLE:
        return Double.MAX_VALUE;
      case STRING:
      case BINARY:
        return MAX_UTF8;
      default:
        throw new UnsupportedOperationException(String.format(
            "No maximum value for %s: %s",
            column.getName(), column.getType()));
    }
  }

  private static Range<?> allValues(ColumnSchema column) {
    switch (column.getType()) {
      case INT8:
        return Ranges.closedOpen(Byte.MIN_VALUE, Byte.MAX_VALUE);
      case INT16:
        return Ranges.closedOpen(Short.MIN_VALUE, Short.MAX_VALUE);
      case INT32:
        return Ranges.closedOpen(Integer.MIN_VALUE, Integer.MAX_VALUE);
      case INT64:
        return Ranges.closedOpen(Long.MIN_VALUE, Long.MAX_VALUE);
      case FLOAT:
        return Ranges.closedOpen(Float.MIN_VALUE, Float.MAX_VALUE);
      case DOUBLE:
        return Ranges.closedOpen(Double.MIN_VALUE, Double.MAX_VALUE);
      case STRING:
      case BINARY:
        return Ranges.closedOpen(MIN_UTF8, MAX_UTF8);
      default:
        throw new UnsupportedOperationException(String.format(
            "Cannot build a default range for %s: %s",
            column.getName(), column.getType()));
    }
  }

  private static byte[] copyFromByteBuffer(ByteBuffer buf) {
    byte[] copy = new byte[buf.remaining()];
    buf.mark();
    buf.get(copy).reset();
    return copy;
  }

  public static <E> E makeRecord(RowResult result, Class<E> recordClass, Schema schema) {
    E record = newRecordInstance(recordClass, schema);

    if (record instanceof IndexedRecord) {
      fillIndexed(result, (IndexedRecord) record, schema);
    } else {
      fillReflect(result, record, schema);
    }

    return record;
  }

  private static void fillIndexed(RowResult result, IndexedRecord record, Schema schema) {
    List<ColumnSchema> columns = result.getColumnProjection().getColumns();
    for (int ordinal = 0; ordinal < columns.size(); ordinal += 1) {
      ColumnSchema column = columns.get(ordinal);
      Object value = getValue(column, ordinal, result);
      int pos = schema.getField(column.getName()).pos();
      record.put(pos, value);
    }
  }

  private static void fillReflect(RowResult result, Object record, Schema schema) {
    List<ColumnSchema> columns = result.getColumnProjection().getColumns();
    for (int ordinal = 0; ordinal < columns.size(); ordinal += 1) {
      ColumnSchema column = columns.get(ordinal);
      Object value = getValue(column, ordinal, result);
      Schema.Field field = schema.getField(column.getName());
      ReflectData.get().setField(record, field.name(), field.pos(), value);
    }
  }

  private static Object getValue(ColumnSchema column, int ordinal,
                                 RowResult result) {
    switch (column.getType()) {
      case BOOL:
        return result.getBoolean(ordinal);
      case INT8:
        return (int) result.getByte(ordinal);
      case INT16:
        return (int) result.getShort(ordinal);
      case INT32:
        return result.getInt(ordinal);
      case INT64:
        return result.getLong(ordinal);
      case FLOAT:
        return result.getFloat(ordinal);
      case DOUBLE:
        return result.getDouble(ordinal);
      case STRING:
        return result.getString(ordinal);
      case BINARY:
        // already a ByteBuffer. fixed is not supported.
        return result.getBinary(ordinal);
      default:
        throw new IllegalArgumentException("Unknown type: " + column);
    }
  }

  @SuppressWarnings("unchecked")
  public static <E> E newRecordInstance(Class<E> recordClass, Schema schema) {
    if (recordClass != GenericData.Record.class && !recordClass.isInterface()) {
      E record = (E) ReflectData.newInstance(recordClass, schema);
      if (record != null) {
        return record;
      }
    }
    return (E) new GenericData.Record(schema);
  }

  private static Joiner.MapJoiner MAP_JOINER = Joiner.on('&').withKeyValueSeparator("=");
  private static Splitter.MapSplitter MAP_SPLITTER = Splitter.on('&').withKeyValueSeparator("=");

  public static String mapToString(Map<String, String> map) {
    return MAP_JOINER.join(map);
  }

  public static Map<String, String> stringToMap(String string) {
    return MAP_SPLITTER.split(string);
  }
}
