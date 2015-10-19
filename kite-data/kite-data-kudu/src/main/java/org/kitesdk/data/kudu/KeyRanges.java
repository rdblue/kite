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

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.kitesdk.data.impl.Accessor;
import org.kitesdk.data.spi.AbstractRefinableView;
import org.kitesdk.data.spi.Constraints;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.spi.Pair;
import org.kitesdk.data.spi.filesystem.MultiLevelIterator;
import org.kitesdk.data.spi.predicates.In;
import org.kitesdk.data.spi.predicates.Predicates;
import org.kitesdk.data.spi.predicates.Range;
import org.kitesdk.data.spi.predicates.Ranges;
import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.kududb.client.PartialRow;
import java.util.Iterator;
import java.util.List;

public class KeyRanges implements Iterable<Pair<PartialRow, PartialRow>> {
  private final Schema schema;
  private final List<In<Object>> inPredicates;
  private final List<Predicate<?>> residuals;
  private final Range<Object> range;

  @SuppressWarnings("unchecked")
  public KeyRanges(Schema schema, AbstractRefinableView<?> view) {
    this.schema = schema;

    ImmutableList.Builder<In<Object>> predicates = ImmutableList.builder();
    Range<Object> range = null;

    // accumulate in predicates until the first range
    Constraints constraints = view.getConstraints();
    List<FieldPartitioner> fps = Accessor.getDefault()
        .getFieldPartitioners(constraints.getPartitionStrategy());
    for (FieldPartitioner<?, ?> fp : fps) {
      // combine the column's predicates, but allow the partition predicate to
      // be more relaxed so callers have control over the number of scanners.
      Predicate predicate = getPartitionPredicate(fp,
          constraints.get(fp.getSourceName()),
          constraints.get(fp.getName()));

      // missing constraint levels are turned into a range query for all values
      if (predicate instanceof In) {
        predicates.add((In<Object>) predicate);
      } else {
        // exists and null get turned into a range of all values
        range = KuduUtil.addMissingEndpoints(
            schema.getColumn(fp.getName()),
            (predicate instanceof Range ? (Range) predicate : Ranges.all()));
        break;
      }
    }

    this.inPredicates = predicates.build();
    this.range = range;

    // TODO: build residuals
    ImmutableList.Builder<Predicate<?>> residuals = ImmutableList.builder();
    this.residuals = residuals.build();
  }

  private static <S, T> Predicate<T> getPartitionPredicate(
      FieldPartitioner<S, T> fp, Predicate<S> sourcePredicate,
      Predicate<T> partitionPredicate) {
    Predicate<T> projectedPredicate = fp.project(sourcePredicate);
    if (partitionPredicate instanceof In) {
      // use the most specific predicate possible by intersecting
      return Constraints.combine(partitionPredicate, projectedPredicate);
    } else if (partitionPredicate instanceof Range) {
      if (projectedPredicate instanceof Range) {
        return ((Range<T>) partitionPredicate).intersection(
            (Range<T>) projectedPredicate);
      }
      // let the partition predicate override the source predicate's projection
      // to allow users to specify partition ranges for efficiency. for example,
      // source=1,3,5 and partition=[1,5] could be used to scan source values
      // 2 and 4 rather than instantiating 3 different scanners
      return partitionPredicate;
    } else {
      // either exists or null: use the source predicate's projection
      return projectedPredicate;
    }
  }

  // Cases to worry about:
  // 1. Kudu hash bucketing?
  // 2. What about related columns? This will currently create the
  //    cross-product (one column should be in the cross-product and dependent
  //    columns should be added at the same time it is determined)

  @Override
  public Iterator<Pair<PartialRow, PartialRow>> iterator() {
    return new KeyRangeIterator();
  }

  private class KeyRangeIterator
      implements Iterator<Pair<PartialRow, PartialRow>> {
    private final Iterator<List<Object>> products;

    public KeyRangeIterator() {
      if (inPredicates.isEmpty()) {
        this.products = Iterators.singletonIterator(
            (List<Object>) ImmutableList.of());
      } else {
        this.products = new CrossProduct(inPredicates);
      }
    }

    @Override
    public boolean hasNext() {
      return products.hasNext();
    }

    @Override
    public Pair<PartialRow, PartialRow> next() {
      List<Object> product = products.next();
      PartialRow lower = new PartialRow(schema);
      PartialRow upper = new PartialRow(schema);

      int n = product.size();
      for (int i = 0; i < product.size(); i += 1) {
        ColumnSchema column = schema.getColumnByIndex(i);
        KuduUtil.addValue(lower, column, i, product.get(i));
        if (range == null && (i + 1) == product.size()) {
          // adjust the upper bound to make a range
          KuduUtil.addValue(upper, column, i,
              KuduUtil.toExclusiveValue(column, product.get(i)));
        } else {
          KuduUtil.addValue(upper, column, i, product.get(i));
        }
      }

      // if the range is null, the last value was turned into a range above
      // if not null, then add the range's endpoints
      if (range != null) {
        ColumnSchema rangeColumn = schema.getColumnByIndex(n);

        // if it isn't exclusive already, make an exclusive value
        Object upperEndpoint = range.isUpperBoundClosed() ?
            KuduUtil.toExclusiveValue(rangeColumn, range.upperEndpoint()) :
            range.upperEndpoint();

        KuduUtil.addValue(upper, rangeColumn, n, upperEndpoint);
        KuduUtil.addValue(lower, rangeColumn, n, range.lowerEndpoint());
      }

      return Pair.of(lower, upper);
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException(
          "Remove is not supported for key range iterators");
    }
  }

  private static class CrossProduct extends MultiLevelIterator<Object> {
    private final List<In<Object>> inPredicates;

    public CrossProduct(List<In<Object>> inPredicates) {
      super(inPredicates.size());
      this.inPredicates = inPredicates;
    }

    @Override
    public Iterable<Object> getLevel(List<Object> current) {
      return Predicates.asSet(inPredicates.get(current.size()));
    }
  }
}
