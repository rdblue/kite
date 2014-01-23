/*
 * Copyright 2013 Cloudera Inc.
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

package org.kitesdk.data.spi;

import org.kitesdk.data.RefineableView;
import org.kitesdk.data.View;

// TODO: delete this class since markers can be replaced by the single-field variants, or Tuples
interface RangeView<E> extends RefineableView<E> {

  /**
   * Returns whether a {@link Marker} is in this {@code View}
   *
   * @param marker a {@code Marker}
   * @return true if {@code marker} is in the partition space of this view.
   */
  boolean contains(Marker marker);

  /**
   * Creates a sub-{@code View}, from the {@code start} {@link Marker} to the
   * end of this {@code View}.
   *
   * The returned View is inclusive: the partition space contained by the start
   * Marker is included.
   *
   * {@code start} must be contained by this {@code View}, as determined by
   * {@link #contains(Marker)}.
   *
   * @param start a starting {@code Marker}
   * @return a {@code View} for the range
   *
   * @throws IllegalArgumentException If {@code start} is null or not in this
   *                                  {@code View}.
   */
  RangeView<E> from(Marker start);

  /**
   * Creates a sub-{@code View}, from after the {@code start} {@link Marker} to
   * the end of this {@code View}.
   *
   * The returned View is exclusive: the partition space contained by the start
   * Marker is not included.
   *
   * {@code start} must be contained by this {@code View}, as determined by
   * {@link #contains(Marker)}.
   *
   * @param start a starting {@code Marker}
   * @return a {@code View} for the range
   *
   * @throws IllegalArgumentException If {@code start} is null or not in this
   *                                  {@code View}.
   */
  RangeView<E> fromAfter(Marker start);

  /**
   * Creates a sub-{@code View}, from the start of this {@code View} to the
   * {@code end} {@link Marker}.
   *
   * The returned View is inclusive: the space contained by the end Marker is
   * included.
   *
   * {@code end} must be contained by this {@code View}, as determined by
   * {@link #contains(Marker)}.
   *
   * @param end an ending {@code Marker}
   * @return a {@code View} for the range
   *
   * @throws IllegalArgumentException If {@code end} is null or not in this
   *                                  {@code View}.
   */
  RangeView<E> to(Marker end);

  /**
   * Creates a sub-{@code View}, from the start of this {@code View} to before
   * the {@code end} {@link Marker}.
   *
   * The returned View is exclusive: the space contained by the end Marker is
   * not included.
   *
   * {@code end} must be contained by this {@code View}, as determined by
   * {@link #contains(Marker)}.
   *
   * @param end an ending {@code Marker}
   * @return a {@code View} for the range
   *
   * @throws IllegalArgumentException If {@code end} is null or not in this
   *                                  {@code View}.
   */
  RangeView<E> toBefore(Marker end);

  /**
   * Creates a sub-{@code View} from the partition space contained by a partial
   * {@link Marker}.
   *
   * The returned View will contain all partitions that match the values from
   * {@code Marker} for a {@link org.kitesdk.data.PartitionStrategy}. For example, consider the
   * following {@code Marker} and {@code PartitionStrategy}:
   * <pre>
   * partial = new Marker.Builder("time", 1380841757896).get();
   * mmdd = new PartitionStrategy.Builder().month("time").day("time").get();
   * </pre>
   *
   * The {@code Marker} contains partitions only given a concrete
   * {@code PartitionStrategy}, which determines the values that will be
   * matched. Above, the {@code Marker} contains all October 3rd partitions.
   * But, if the {@code PartitionStrategy} contained a year field, it would
   * contain only October 3rd for 2013.
   *
   * To limit how specific a {@code Marker} is, it can be constructed using
   * concrete values rather than source values. This example limits the partial
   * to just October, even though the partition strategy includes the day:
   * <pre>
   * partial = new Marker.Builder("month", OCTOBER).get();
   * mmdd = new PartitionStrategy.Builder().month("time").day("time").get();
   * </pre>
   *
   * {@code partial} must be contained by this {@code View}, as determined by
   * {@link #contains(Marker)}.
   *
   * @param partial a partial {@code Marker}
   * @return a {@code View} of the partition space under {@code partial}.
   *
   * @throws IllegalArgumentException If {@code partial} is null or not in this
   *                                  {@code View}.
   */
  RangeView<E> of(Marker partial);
}
