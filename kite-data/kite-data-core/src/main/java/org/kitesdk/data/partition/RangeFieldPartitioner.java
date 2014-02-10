/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data.partition;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.BoundType;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.DiscreteDomains;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.List;

import java.util.Set;
import org.kitesdk.data.FieldPartitioner;
import com.google.common.annotations.Beta;
import com.google.common.base.Objects;
import org.kitesdk.data.spi.Predicates;

@Beta
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value={
        "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE",
        "SE_COMPARATOR_SHOULD_BE_SERIALIZABLE"},
    justification="False positive due to generics.")
public class RangeFieldPartitioner extends FieldPartitioner<String, String> {

  private final List<String> upperBounds;

  // lazily constructed DiscreteDomain for upper bounds; use domain()
  private RangeDomain domain;

  public RangeFieldPartitioner(String name, String... upperBounds) {
    super(name, String.class, String.class, upperBounds.length);
    this.upperBounds = Arrays.asList(upperBounds);
  }

  @Override
  public String apply(String value) {
    // always return the same String object so identity comparison can be used
    for (String upper : upperBounds) {
      if (value.compareTo(upper) <= 0) {
        return upper;
      }
    }
    throw new IllegalArgumentException(value + " is outside bounds");
  }

  @Override
  @Deprecated
  public String valueFromString(String stringValue) {
    return stringValue;
  }

  @Override
  public Predicate<String> project(Predicate<String> predicate) {
    if (predicate instanceof Predicates.Exists) {
      return Predicates.exists();
    } else if (predicate instanceof Predicates.In) {
      return ((Predicates.In<String>) predicate).transform(this);
    } else if (predicate instanceof Range) {
      // must use a closed range:
      //   if this( abc ) => b then this( acc ) => b, so b must be included
      return Predicates.in(
          Predicates.transformClosed((Range<String>) predicate, this)
              .asSet(domain()));
    } else {
      return null;
    }
  }

  @Override
  public Predicate<String> projectSatisfied(Predicate<String> predicate) {
    if (predicate instanceof Predicates.Exists) {
      return Predicates.exists();
    } else if (predicate instanceof Predicates.In) {
      // not possible to check all inputs to the predicate
      return null;
    } else if (predicate instanceof Range) {
      Range<String> transformed = Predicates.transformClosedConservative(
          (Range<String>) predicate, this, domain());
      if (transformed != null) {
        return Predicates.in(transformed.asSet(domain()));
      }
    }
    return null;
  }

  public List<String> getUpperBounds() {
    return upperBounds;
  }

  private RangeDomain domain() {
    if (domain == null) {
      this.domain = new RangeDomain();
    }
    return domain;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || !getClass().equals(o.getClass())) {
      return false;
    }
    RangeFieldPartitioner that = (RangeFieldPartitioner) o;
    return Objects.equal(this.getName(), that.getName()) &&
        Objects.equal(this.upperBounds, that.upperBounds);
  }

  @Override
  public int compare(String o1, String o2) {
    return apply(o1).compareTo(apply(o2));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getName(), upperBounds);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("name", getName())
        .add("upperBounds", upperBounds).toString();
  }

  /**
   * A DiscreteDomain for this partitioner's set of upper bounds.
   *
   * Used for Range#asSet, Predicates.transformClosed,
   * Predicates.excludeEndpoints
   *
   * This DiscreteDomain will throw IllegalArgumentException for values that
   * are after the last upper bound, and will throw IndexOutOfBoundsException
   * when next or previous is called on the last or first bound
   */
  private class RangeDomain extends DiscreteDomain<String> {
    @Override
    public String next(String value) {
      // using apply ensures indexOf returns a real index
      int nextIndex = upperBounds.indexOf(apply(value)) + 1;
      return nextIndex == upperBounds.size() ? null : upperBounds.get(nextIndex);
    }

    @Override
    public String previous(String value) {
      // using apply ensures indexOf returns a real index
      int index = upperBounds.indexOf(apply(value));
      return index == 0 ? null : upperBounds.get(index - 1);
    }

    @Override
    public long distance(String start, String end) {
      // using apply ensures indexOf returns a real index
      return (upperBounds.indexOf(apply(end)) -
          upperBounds.indexOf(apply(start)));
    }

    @Override
    public String minValue() {
      return upperBounds.get(0);
    }

    @Override
    public String maxValue() {
      DiscreteDomains.integers();
      return upperBounds.get(upperBounds.size() - 1);
    }
  }
}
