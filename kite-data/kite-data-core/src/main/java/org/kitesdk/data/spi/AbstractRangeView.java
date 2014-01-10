/*
 * Copyright 2013 Cloudera.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.spi;

import com.google.common.base.Preconditions;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import com.google.common.base.Objects;

import javax.annotation.concurrent.Immutable;
import org.kitesdk.data.View;

/**
 * A common View base class to simplify implementations of Views created from ranges.
 *
 * @param <E>
 *      The type of entities stored in the {@code Dataset} underlying this
 *      {@code View}.
 * @since 0.9.0
 */
@Immutable
public abstract class AbstractRangeView<E> implements RangeView<E> {

  protected final Dataset<E> dataset;
  protected final MarkerComparator comparator;
  protected final RangePredicate predicate;

  // This class is Immutable and must be thread-safe
  protected final ThreadLocal<StorageKey> keys;

  protected AbstractRangeView(Dataset<E> dataset) {
    this.dataset = dataset;
    final DatasetDescriptor descriptor = dataset.getDescriptor();
    if (descriptor.isPartitioned()) {
      this.comparator = new MarkerComparator(descriptor.getPartitionStrategy());
      this.predicate = RangePredicates.all(comparator);
      this.keys = new ThreadLocal<StorageKey>() {
        @Override
        protected StorageKey initialValue() {
          return new StorageKey(descriptor.getPartitionStrategy());
        }
      };
    } else {
      // use undefined predicate, which handles inappropriate calls to range methods
      this.comparator = null;
      this.predicate = RangePredicates.undefined();
      this.keys = null; // not used
    }
  }

  protected AbstractRangeView(AbstractRangeView<E> view, RangePredicate predicate) {
    this.dataset = view.dataset;
    this.comparator = view.comparator;
    this.predicate = predicate;
    // thread-safe, so okay to reuse when views share a partition strategy
    this.keys = view.keys;
  }

  protected abstract AbstractRangeView<E> filter(RangePredicate p);

  @Override
  public Dataset<E> getDataset() {
    return dataset;
  }

  @Override
  public boolean deleteAll() {
    throw new UnsupportedOperationException(
        "This Dataset does not support deletion");
  }

  @Override
  public boolean contains(E entity) {
    if (dataset.getDescriptor().isPartitioned()) {
      return predicate.apply(keys.get().reuseFor(entity));
    } else {
      return true;
    }
  }

  @Override
  public boolean contains(Marker marker) {
    return predicate.apply(marker);
  }

  @Override
  public AbstractRangeView<E> from(Marker start) {
    Preconditions.checkState(comparator != null, "Undefined range: no PartitionStrategy");
    return filter(RangePredicates.and(predicate, RangePredicates.from(comparator, start)));
  }

  @Override
  public AbstractRangeView<E> fromAfter(Marker start) {
    Preconditions.checkState(comparator != null, "Undefined range: no PartitionStrategy");
    return filter(RangePredicates.and(predicate, RangePredicates.fromAfter(comparator, start)));
  }

  @Override
  public AbstractRangeView<E> to(Marker end) {
    Preconditions.checkState(comparator != null, "Undefined range: no PartitionStrategy");
    return filter(RangePredicates.and(predicate, RangePredicates.to(comparator, end)));
  }

  @Override
  public AbstractRangeView<E> toBefore(Marker end) {
    Preconditions.checkState(comparator != null, "Undefined range: no PartitionStrategy");
    return filter(RangePredicates.and(predicate, RangePredicates.toBefore(comparator, end)));
  }

  @Override
  public AbstractRangeView<E> of(Marker partial) {
    Preconditions.checkState(comparator != null, "Undefined range: no PartitionStrategy");
    return filter(RangePredicates.and(predicate, RangePredicates.of(comparator, partial)));
  }

  @Override
  public View<E> with(String name, Object value) {
    Preconditions.checkState(comparator != null, "Undefined range: no PartitionStrategy");
    return filter(RangePredicates.and(predicate, RangePredicates.with(comparator, name, value)));
  }

  @Override
  public View<E> from(String name, Object value) {
    Preconditions.checkState(comparator != null, "Undefined range: no PartitionStrategy");
    return filter(RangePredicates.and(predicate, RangePredicates.from(comparator, name, value)));
  }

  @Override
  public View<E> fromAfter(String name, Object value) {
    Preconditions.checkState(comparator != null, "Undefined range: no PartitionStrategy");
    return filter(RangePredicates.and(predicate, RangePredicates.fromAfter(comparator, name, value)));
  }

  @Override
  public View<E> to(String name, Object value) {
    Preconditions.checkState(comparator != null, "Undefined range: no PartitionStrategy");
    return filter(RangePredicates.and(predicate, RangePredicates.to(comparator, name, value)));
  }

  @Override
  public View<E> toBefore(String name, Object value) {
    Preconditions.checkState(comparator != null, "Undefined range: no PartitionStrategy");
    return filter(RangePredicates.and(predicate, RangePredicates.toBefore(comparator, name, value)));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if ((o == null) || !Objects.equal(this.getClass(), o.getClass())) {
      return false;
    }

    AbstractRangeView that = (AbstractRangeView) o;
    return (Objects.equal(this.dataset, that.dataset) &&
        Objects.equal(this.predicate, that.predicate));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getClass(), dataset, predicate);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("dataset", dataset)
        .add("predicate", predicate)
        .toString();
  }
}
