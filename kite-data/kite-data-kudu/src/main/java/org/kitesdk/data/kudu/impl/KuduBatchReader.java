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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.kitesdk.data.DatasetOperationException;
import org.kitesdk.data.View;
import org.kitesdk.data.kudu.KeyRanges;
import org.kitesdk.data.kudu.KuduUtil;
import org.kitesdk.data.spi.AbstractDatasetReader;
import org.kitesdk.data.spi.AbstractRefinableView;
import org.kitesdk.data.spi.EntityAccessor;
import org.kitesdk.data.spi.Pair;
import org.kitesdk.data.spi.ReaderWriterState;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduScanner;
import org.kududb.client.KuduTable;
import org.kududb.client.PartialRow;
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
  private final Iterator<Pair<PartialRow, PartialRow>> ranges;
  private ReaderWriterState state = ReaderWriterState.NEW;
  private KuduScanner scanner = null;
  private Iterator<E> currentRows = null;

  public KuduBatchReader(KuduClient client, KuduTable table, View<E> view) {
    this.client = client;
    this.table = table;
    this.accessor = ((AbstractRefinableView<E>) view).getAccessor();
    this.entityPredicate = ((AbstractRefinableView<E>) view).getConstraints()
        .toEntityPredicate(accessor);
    if (view instanceof AbstractRefinableView) {
      if (!((AbstractRefinableView) view).getConstraints().isUnbounded()) {
        this.ranges = new KeyRanges(
            table.getSchema(), (AbstractRefinableView) view).iterator();
      } else {
        this.ranges = Iterators.emptyIterator();
      }
    } else {
      this.ranges = Iterators.emptyIterator();
    }
  }

  @Override
  public void initialize() {
    Preconditions.checkState(state == ReaderWriterState.NEW,
        "Cannot initialize reader in state: " + state);

    // if there is not at least one range, do a full table scan
    if (ranges.hasNext()) {
      this.scanner = nextScanner();
    } else {
      this.scanner = fullTableScanner();
    }

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
                    new ToAvro<>(accessor.getReadSchema(), accessor.getType())),
                entityPredicate);
          } else if (ranges.hasNext()) {
            this.scanner = nextScanner();
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
      return KuduUtil.makeRecord(result, recordClass, schema);
    }
  }

  private List<String> columns(Schema schema) {
    List<String> columns = Lists.newArrayList();
    for (Schema.Field field : schema.getFields()) {
      columns.add(field.name());
    }
    return columns;
  }

  private KuduScanner fullTableScanner() {
    return client.newScannerBuilder(table)
        .setProjectedColumnNames(columns(accessor.getReadSchema()))
        .build();
  }

  private KuduScanner nextScanner() {
    Pair<PartialRow, PartialRow> range = ranges.next();
    return client.newScannerBuilder(table)
        .setProjectedColumnNames(columns(accessor.getReadSchema()))
        .lowerBound(range.first())
        .exclusiveUpperBound(range.second())
        .build();
  }
}
