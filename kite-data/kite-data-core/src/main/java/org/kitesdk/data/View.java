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
package org.kitesdk.data;

import javax.annotation.concurrent.Immutable;

/**
 * A {@code View} is a subset of a {@link Dataset}.
 *
 * A {@code View} defines a space of potential storage keys, or a partition
 * space. Views can be created from ranges, partial keys, or the union of other
 * views.
 *
 * @param <E>
 *      The type of entities stored in the {@code Dataset} underlying this
 *      {@code View}.
 * @since 0.9.0
 */
@Immutable
public interface View<E> {

  /**
   * Returns the underlying {@link org.kitesdk.data.Dataset} that this is a {@code View} of.
   *
   * @return the underlying {@code Dataset}
   */
  Dataset<E> getDataset();

  /**
   * Get an appropriate {@link DatasetReader} implementation based on this
   * {@code View} of the underlying {@code Dataset} implementation.
   *
   * Implementations are free to return different types of readers depending on
   * the disposition of the data. For example, a partitioned dataset may use a
   * different reader than that of a non-partitioned dataset. Clients should not
   * make any assumptions about the returned implementations. Implementations
   * are free to change them at any time.
   *
   * @throws DatasetException
   */
  DatasetReader<E> newReader();

  /**
   * Get an appropriate {@link DatasetWriter} implementation based on this
   * {@code View} of the underlying {@code Dataset} implementation.
   *
   * Implementations are free to return different types of writers depending on
   * the disposition of the data. For example, a partitioned dataset may use a
   * different writer than that of a non-partitioned dataset. Clients should not
   * make any assumptions about the returned implementations. Implementations
   * are free to change them at any time.
   *
   * @throws DatasetException
   */
  DatasetWriter<E> newWriter();

  /**
   * Returns whether an entity {@link Object} is in this {@code View}.
   *
   * @param key an entity {@code Object}
   * @return true if {@code key} is in the partition space of this view.
   */
  boolean contains(E key);

  boolean contains(String[] names, Object... values);

  /**
   * Deletes the data in this {@link View} or throws an {@code Exception}.
   *
   * Implementations may choose to throw {@link UnsupportedOperationException}
   * for deletes that cannot be easily satisfied by the implementation. For
   * example, in a partitioned {@link Dataset}, the implementation may reject
   * deletes that do not align with partition boundaries. Implementations must
   * document what deletes are supported and under what conditions deletes will
   * be rejected.
   *
   * @return true if any data was deleted; false if the view was already empty
   * @throws UnsupportedOperationException
   *          If the requested delete cannot be completed by the implementation
   * @throws org.kitesdk.data.DatasetIOException
   *          If the requested delete failed because of an IOException
   */
  boolean deleteAll();

  /**
   * Returns an Iterable of non-overlapping {@link View} objects that partition
   * the underlying {@link org.kitesdk.data.Dataset} and cover this {@code View}.
   *
   * The returned {@code View} objects are implementation-specific, but should
   * represent reasonable partitions of the underlying {@code Dataset} based on
   * its layout.
   *
   * The data contained by the union of each {@code View} in the Iterable must
   * be a super-set of this {@code View}.
   *
   * Note that partitions are actual partitions under which data is stored.
   * Implementations are encouraged to omit any {@code View} that is empty.
   *
   * This method is intended to be used by classes like InputFormat, which need
   * to enumerate the underlying partitions to create InputSplits.
   *
   * @return
   *      An Iterable of the {@code View} that cover this {@code View}.
   * @throws IllegalStateException
   *      If the underlying {@code Dataset} is not partitioned.
   */
  Iterable<View<E>> getCoveringPartitions();

}
