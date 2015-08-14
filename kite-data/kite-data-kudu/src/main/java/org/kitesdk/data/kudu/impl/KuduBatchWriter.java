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

import com.google.common.base.Preconditions;
import org.apache.avro.util.Utf8;
import org.kitesdk.data.DatasetOperationException;
import org.kitesdk.data.DatasetRecordException;
import org.kitesdk.data.Flushable;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.AbstractDatasetWriter;
import org.kitesdk.data.spi.AbstractRefinableView;
import org.kitesdk.data.spi.Conversions;
import org.kitesdk.data.spi.EntityAccessor;
import org.kitesdk.data.spi.ReaderWriterState;
import org.kududb.ColumnSchema;
import org.kududb.client.BatchResponse;
import org.kududb.client.Insert;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduSession;
import org.kududb.client.KuduTable;
import org.kududb.client.OperationResponse;
import org.kududb.client.PartialRow;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class KuduBatchWriter<E> extends AbstractDatasetWriter<E> implements Flushable {
  private final KuduClient client;
  private final KuduTable table;
  private final View<E> view;
  private ReaderWriterState state = ReaderWriterState.NEW;
  private EntityAccessor<E> accessor = null;
  private KuduSession session = null;
  private List<ColumnSchema> columns = null;

  public KuduBatchWriter(KuduClient client, KuduTable table, View<E> view) {
    this.client = client;
    this.table = table;
    this.view = view;
  }

  @Override
  public void initialize() {
    Preconditions.checkState(state == ReaderWriterState.NEW,
        "Cannot open a writer in state " + state);
    this.accessor = ((AbstractRefinableView<E>) view).getAccessor();
    this.session = client.newSession();
    this.columns = table.getSchema().getColumns();
    this.state = ReaderWriterState.OPEN;
  }

  @Override
  public void write(E entity) {
    Preconditions.checkState(isOpen(), "Cannot write: not open");

    if (!view.includes(entity)) {
      throw new DatasetRecordException(String.format(
          "Cannot write '%s': not in %s", entity, view.getUri()));
    }

    try {
      OperationResponse response = session.apply(buildInsert(entity));
      if (response.hasRowError()) {
        throw new DatasetRecordException(response.getRowError().toString());
      }
    } catch (Exception e) {
      this.state = ReaderWriterState.ERROR;
      throw new DatasetOperationException("Write failed", e);
    }
  }

  @Override
  public void close() {
    if (state == ReaderWriterState.OPEN) {
      this.state = ReaderWriterState.CLOSED;
      List<BatchResponse> results;
      try {
        results = this.session.close();
        this.session = null;
      } catch (Exception e) {
        this.state = ReaderWriterState.ERROR;
        throw new DatasetOperationException(
            "Failed to close session: " + session, e);
      }
      handleResults(results);
    }
  }

  @Override
  public boolean isOpen() {
    return state == ReaderWriterState.OPEN;
  }

  @Override
  public void flush() {
    Preconditions.checkState(isOpen(), "Cannot flush: not open");

    List<BatchResponse> results;
    try {
      results = this.session.flush();
    } catch (Exception e) {
      this.state = ReaderWriterState.ERROR;
      throw new DatasetOperationException(
          "Failed to flush session: " + session, e);
    }
    handleResults(results);
  }

  private Insert buildInsert(E entity) {
    Insert insert = table.newInsert();
    PartialRow row = insert.getRow();

    for (int ordinal = 0; ordinal < columns.size(); ordinal += 1) {
      ColumnSchema column = columns.get(ordinal);
      Object value = accessor.get(entity, column.getName());
      switch (column.getType()) {
        case BOOL:
          row.addBoolean(ordinal, Conversions.makeBoolean(value));
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
            row.addStringUtf8(ordinal, Arrays.copyOf(
                ((Utf8) value).getBytes(),
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

  private byte[] copyFromByteBuffer(ByteBuffer buf) {
    byte[] copy = new byte[buf.remaining()];
    buf.mark();
    buf.get(copy).reset();
    return copy;
  }

  private void handleResults(List<BatchResponse> results) {
    for (BatchResponse response : results) {
      if (response.hasRowErrors()) {
        throw new DatasetOperationException(
            "Write operation failed: " + response.getRowErrors());
      }
    }
  }
}
