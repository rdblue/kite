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

import org.apache.avro.util.Utf8;
import org.kitesdk.data.spi.Conversions;
import org.kitesdk.data.spi.EntityAccessor;
import org.kududb.ColumnSchema;
import org.kududb.client.Insert;
import org.kududb.client.KuduTable;
import org.kududb.client.PartialRow;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class KuduUtil {

  public static <E> Insert buildInsert(E entity, KuduTable table, EntityAccessor<E> accessor) {
    Insert insert = table.newInsert();
    PartialRow row = insert.getRow();
    List<ColumnSchema> columns = table.getSchema().getColumns();
    for (int ordinal = 0; ordinal < columns.size(); ordinal += 1) {
      ColumnSchema column = columns.get(ordinal);
      Object value = accessor.get(entity, column.getName());
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

    return null;
  }

  private static byte[] copyFromByteBuffer(ByteBuffer buf) {
    byte[] copy = new byte[buf.remaining()];
    buf.mark();
    buf.get(copy).reset();
    return copy;
  }
}
