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
import org.kitesdk.data.DatasetOperationException;
import org.kitesdk.data.DatasetRecordException;
import org.kitesdk.data.Flushable;
import org.kitesdk.data.View;
import org.kitesdk.data.kudu.KuduUtil;
import org.kitesdk.data.spi.AbstractDatasetWriter;
import org.kitesdk.data.spi.AbstractRefinableView;
import org.kitesdk.data.spi.EntityAccessor;
import org.kitesdk.data.spi.ReaderWriterState;
import org.kududb.ColumnSchema;
import org.kududb.client.BatchResponse;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduSession;
import org.kududb.client.KuduTable;
import org.kududb.client.OperationResponse;
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
      OperationResponse response = session
          .apply(KuduUtil.buildInsert(entity, table, accessor));

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

  private void handleResults(List<BatchResponse> results) {
    for (BatchResponse response : results) {
      if (response.hasRowErrors()) {
        this.state = ReaderWriterState.ERROR;
        throw new DatasetOperationException(
            "Write operation failed: " + response.getRowErrors());
      }
    }
  }
}
