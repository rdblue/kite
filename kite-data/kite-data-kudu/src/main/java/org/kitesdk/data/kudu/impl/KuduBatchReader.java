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

package org.kitesdk.data.kudu.impl;

import org.kitesdk.shaded.com.google.common.base.Function;
import org.kitesdk.shaded.com.google.common.base.Preconditions;
import org.kitesdk.shaded.com.google.common.base.Predicate;
import org.kitesdk.shaded.com.google.common.collect.Iterators;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.reflect.ReflectData;
import org.kitesdk.data.DatasetOperationException;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.AbstractDatasetReader;
import org.kitesdk.data.spi.AbstractRefinableView;
import org.kitesdk.data.spi.EntityAccessor;
import org.kitesdk.data.spi.ReaderWriterState;
import org.kududb.ColumnSchema;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduScanner;
import org.kududb.client.KuduTable;
import org.kududb.client.RowResult;
import org.kududb.client.RowResultIterator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class KuduBatchReader<E> extends AbstractDatasetReader<E> {

  private final KuduClient client;
  private final KuduTable table;
  private final EntityAccessor<E> accessor;
  private final Predicate<E> entityPredicate;
  private ReaderWriterState state = ReaderWriterState.NEW;
  private KuduScanner scanner = null;
  private Iterator<E> currentRows = null;

  public KuduBatchReader(KuduClient client, KuduTable table, View<E> view) {
    this.client = client;
    this.table = table;
    this.accessor = ((AbstractRefinableView<E>) view).getAccessor();
    this.entityPredicate = ((AbstractRefinableView<E>) view).getConstraints()
        .toEntityPredicate(accessor);
  }

  @Override
  public void initialize() {
    Preconditions.checkState(state == ReaderWriterState.NEW,
        "Cannot initialize reader in state: " + state);

    this.scanner = client.newScannerBuilder(table)
        .build();
    this.state = ReaderWriterState.OPEN;
  }

  @Override
  public boolean hasNext() {
    Preconditions.checkState(state == ReaderWriterState.OPEN,
        "Cannot read in state: " + state);
    try {
      while (true) {
        if (currentRows == null) {
          if (scanner.hasMoreRows()) {
            RowResultIterator rows = scanner.nextRows();
            if (rows == null) {
              return false;
            }
            this.currentRows = Iterators.filter(Iterators.transform(rows,
                new ToAvro<E>(accessor.getReadSchema(), accessor.getType())),
                entityPredicate);
          } else {
            return false;
          }
        } else {
          if (currentRows.hasNext()) {
            return true;
          } else {
            this.currentRows = null;
          }
        }
      }
    } catch (Exception e) {
      throw new DatasetOperationException("Cannot read", e);
    }
  }

  @Override
  public E next() {
    Preconditions.checkState(state == ReaderWriterState.OPEN,
        "Cannot read in state: " + state);

    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    return currentRows.next();
  }

  @Override
  public void close() {
    if (state == ReaderWriterState.OPEN) {
      this.state = ReaderWriterState.CLOSED;
      try {
        scanner.close();
      } catch (Exception e) {
        throw new DatasetOperationException(
            "Failed to close scanner: " + scanner, e);
      }
      scanner = null;
    }
  }

  @Override
  public boolean isOpen() {
    return state == ReaderWriterState.OPEN;
  }

  private static class ToAvro<E> implements Function<RowResult, E> {
    private final Schema schema;
    private final Class<E> recordClass;

    public ToAvro(Schema schema, Class<E> recordClass) {
      this.schema = schema;
      this.recordClass = recordClass;
    }

    @Override
    public E apply(RowResult result) {
      return makeRecord(result);
    }

    private E makeRecord(RowResult result) {
      E record = newRecordInstance();

      if (record instanceof IndexedRecord) {
        fillIndexed(result, (IndexedRecord) record);
      } else {
        fillReflect(result, record);
      }

      return record;
    }

    private void fillIndexed(RowResult result, IndexedRecord record) {
      List<ColumnSchema> columns = result.getColumnProjection().getColumns();
      for (int ordinal = 0; ordinal < columns.size(); ordinal += 1) {
        ColumnSchema column = columns.get(ordinal);
        Object value = getValue(column, ordinal, result);
        int pos = schema.getField(column.getName()).pos();
        record.put(pos, value);
      }
    }

    private void fillReflect(RowResult result, Object record) {
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
    private E newRecordInstance() {
      if (recordClass != GenericData.Record.class && !recordClass.isInterface()) {
        E record = (E) ReflectData.newInstance(recordClass, schema);
        if (record != null) {
          return record;
        }
      }
      return (E) new GenericData.Record(schema);
    }
  }
}
