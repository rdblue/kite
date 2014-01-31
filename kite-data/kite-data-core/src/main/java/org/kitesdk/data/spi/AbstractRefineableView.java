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

import com.google.common.base.Predicate;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import com.google.common.base.Objects;

import javax.annotation.concurrent.Immutable;

import org.kitesdk.data.RefineableView;
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
public abstract class AbstractRefineableView<E> implements RefineableView<E> {

  protected final Dataset<E> dataset;
  protected final MarkerComparator comparator;
  protected final Constraints constraints;
  protected final Predicate<E> entityTest;

  // This class is Immutable and must be thread-safe
  protected final ThreadLocal<StorageKey> keys;

  protected AbstractRefineableView(Dataset<E> dataset) {
    this.dataset = dataset;
    final DatasetDescriptor descriptor = dataset.getDescriptor();
    if (descriptor.isPartitioned()) {
      this.comparator = new MarkerComparator(descriptor.getPartitionStrategy());
      this.keys = new ThreadLocal<StorageKey>() {
        @Override
        protected StorageKey initialValue() {
          return new StorageKey(descriptor.getPartitionStrategy());
        }
      };
    } else {
      this.comparator = null;
      this.keys = null;
    }
    this.constraints = new Constraints();
    this.entityTest = constraints.toEntityPredicate();
  }

  protected AbstractRefineableView(AbstractRefineableView<E> view, Constraints constraints) {
    this.dataset = view.dataset;
    this.comparator = view.comparator;
    this.constraints = constraints;
    this.entityTest = constraints.toEntityPredicate();
    // thread-safe, so okay to reuse when views share a partition strategy
    this.keys = view.keys;
  }

  protected abstract AbstractRefineableView<E> filter(Constraints c);

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
    return entityTest.apply(entity);
  }

  @Override
  public AbstractRefineableView<E> with(String name) {
    return filter(constraints.with(name));
  }

  @Override
  public AbstractRefineableView<E> with(String name, Object value) {
    return filter(constraints.with(name, value));
  }

  @Override
  public AbstractRefineableView<E> from(String name, Object value) {
    return filter(constraints.from(name, (Comparable) value));
  }

  @Override
  public AbstractRefineableView<E> fromAfter(String name, Object value) {
    return filter(constraints.fromAfter(name, (Comparable) value));
  }

  @Override
  public AbstractRefineableView<E> to(String name, Object value) {
    return filter(constraints.to(name, (Comparable) value));
  }

  @Override
  public AbstractRefineableView<E> toBefore(String name, Object value) {
    return filter(constraints.toBefore(name, (Comparable) value));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if ((o == null) || !Objects.equal(this.getClass(), o.getClass())) {
      return false;
    }

    AbstractRefineableView that = (AbstractRefineableView) o;
    return (Objects.equal(this.dataset, that.dataset) &&
        Objects.equal(this.constraints, that.constraints));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getClass(), dataset, constraints);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("dataset", dataset)
        .add("constraints", constraints)
        .toString();
  }
}
