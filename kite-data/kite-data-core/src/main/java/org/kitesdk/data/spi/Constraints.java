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

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;
import com.google.common.collect.Sets;
import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.apache.avro.generic.GenericRecord;
import org.kitesdk.data.FieldPartitioner;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.partition.CalendarFieldPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A set of simultaneous constraints.
 */
@Immutable
public class Constraints<E> implements Predicate<E> {

  private static final Logger LOG = LoggerFactory.getLogger(Constraints.class);

  private final Map<String, Predicate> constraints;

  public static class Exists<T> implements Predicate<T> {
    public static final Exists INSTANCE = new Exists();

    private Exists() {
    }

    @Override
    public boolean apply(@Nullable T value) {
      return (value != null);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).toString();
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> Exists<T> exists() {
    return (Exists<T>) Exists.INSTANCE;
  }

  public static class In<T> implements Predicate<T> {
    // ImmutableSet entries are non-null
    private final ImmutableSet<T> set;

    private In(Iterable<T> values) {
      this.set = ImmutableSet.copyOf(values);
      Preconditions.checkArgument(set.size() > 0, "No values to match");
    }

    private In(T... values) {
      this.set = ImmutableSet.copyOf(values);
    }

    @Override
    public boolean apply(@Nullable T test) {
      // Set#contains may throw NPE, depending on implementation
      return (test != null) && set.contains(test);
    }

    public In<T> filter(Predicate<T> predicate) {
      try {
        return new In<T>(Iterables.filter(set, predicate));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Filter predicate produces empty set", e);
      }
    }

    public <V> In<V> transform(Function<T, V> function) {
      return new In<V>(Iterables.transform(set, function));
    }

    Set<T> getSet() {
      return set;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).add("set", set).toString();
    }
  }

  public static <T> In<T> in(Set<T> set) {
    return new In<T>(set);
  }

  // This should be a method on Range, like In#transform.
  // Unfortunately, Range is final so we will probably need to re-implement it.
  @SuppressWarnings("unchecked")
  public static <S extends Comparable, T extends Comparable>
  Range<T> rangeTransformClosed(Range<S> range, Function<S, T> function) {
    if (range.hasLowerBound()) {
      if (range.hasUpperBound()) {
        return Ranges.closed(
            function.apply(range.lowerEndpoint()),
            function.apply(range.upperEndpoint()));
      } else {
        return Ranges.atLeast(function.apply(range.lowerEndpoint()));
      }
    } else if (range.hasUpperBound()) {
      return Ranges.atMost(function.apply(range.upperEndpoint()));
    } else {
      return (Range<T>) Ranges.all();
    }
  }

  public Constraints() {
    this.constraints = ImmutableMap.of();
  }

  private Constraints(Map<String, Predicate> constraints,
                      String name, Predicate predicate) {
    Map<String, Predicate> copy = Maps.newHashMap(constraints);
    copy.put(name, predicate);
    this.constraints = ImmutableMap.copyOf(copy);
  }

  @Override
  public boolean apply(@Nullable E value) {
    return contains(value);
  }

  @SuppressWarnings("unchecked")
  public boolean contains(@Nullable E entity) {
    if (entity == null) {
      return false;
    }
    // check each constraint and fail immediately
    for (Map.Entry<String, Predicate> entry : constraints.entrySet()) {
      if (!entry.getValue().apply(get(entity, entry.getKey()))) {
        return false;
      }
    }
    // all constraints were satisfied
    return true;
  }

  @SuppressWarnings("unchecked")
  public boolean matchesKey(StorageKey key) {
    if (key == null) {
      return false;
    }

    PartitionStrategy strategy = key.getPartitionStrategy();
    // The constraints are in terms of the data, and the partition functions
    // are one-way functions from data to partition fields. This means that we
    // need to translate, using each function, from the data domain to the
    // partition domain (e.g., hash partition function).
    //
    // strategy to consider: hash(time) / month(time) / year(time) / day(time)
    // another: weekday(time) / year(time) / month(time) / day(time)

    // time fields that affect the partition strategy
    Set<String> timeFields = Sets.newHashSet();

    // this is fail-fast: if the key fails a constraint, then drop it
    for (FieldPartitioner fp : strategy.getFieldPartitioners()) {
      Predicate constraint = constraints.get(fp.getSourceName());

      if (constraint == null) {
        // no constraints => anything matches
        continue;
      }

      Object pValue = key.get(fp.getName());

      if (fp instanceof CalendarFieldPartitioner) {
        timeFields.add(fp.getSourceName());
      }

      Predicate projectedConstraint = fp.project(constraint);
      if (projectedConstraint != null && !(projectedConstraint.apply(pValue))) {
        return false;
      }
    }

    // check multi-field time groups
    for (String sourceName : timeFields) {
      Predicate<StorageKey> timePredicate = TimeDomain
          .get(strategy, sourceName)
          .project(constraints.get(sourceName));
      if (timePredicate != null && !timePredicate.apply(key)) {
        return false;
      }
    }

    // if we made it this far, everything passed
    return true;
  }

  public Iterable<MarkerRange> toKeyRanges(PartitionStrategy strategy) {
    return new KeyRangeIterable(strategy, constraints);
  }

  @SuppressWarnings("unchecked")
  public Constraints<E> with(String name, Object... values) {
    if (values.length > 0) {
      checkContained(name, values);
      // this is the most specific constraint and is idempotent under "and"
      return new Constraints<E>(constraints, name, new In<Object>(values));
    } else {
      if (!constraints.containsKey(name)) {
        // no other constraint => add the exists
        return new Constraints<E>(constraints, name, exists());
      } else {
        // satisfied by an existing constraint
        return this;
      }
    }
  }

  public Constraints<E> from(String name, Comparable value) {
    checkContained(name, value);
    Range added = Ranges.atLeast(value);
    if (constraints.containsKey(name)) {
      return new Constraints<E>(constraints, name,
          and(constraints.get(name), added));
    } else {
      return new Constraints<E>(constraints, name, added);
    }
  }

  public Constraints<E> fromAfter(String name, Comparable value) {
    checkContained(name, value);
    Range added = Ranges.greaterThan(value);
    if (constraints.containsKey(name)) {
      return new Constraints<E>(constraints, name,
          and(constraints.get(name), added));
    } else {
      return new Constraints<E>(constraints, name, added);
    }
  }

  public Constraints<E> to(String name, Comparable value) {
    checkContained(name, value);
    Range added = Ranges.atMost(value);
    if (constraints.containsKey(name)) {
      return new Constraints<E>(constraints, name,
          and(constraints.get(name), added));
    } else {
      return new Constraints<E>(constraints, name, added);
    }
  }

  public Constraints<E> toBefore(String name, Comparable value) {
    checkContained(name, value);
    Range added = Ranges.lessThan(value);
    if (constraints.containsKey(name)) {
      return new Constraints<E>(constraints, name,
          and(constraints.get(name), added));
    } else {
      return new Constraints<E>(constraints, name, added);
    }
  }

  @SuppressWarnings("unchecked")
  private static Predicate and(Predicate previous, Range additional) {
    if (previous instanceof Range) {
      // return the intersection
      return ((Range) previous).intersection(additional);
    } else if (previous instanceof In) {
      // filter the set using the range
      return new In(((In) previous).filter(additional));
    } else if (previous instanceof Exists) {
      // exists is the weakest constraint, satisfied by any existing constraint
      // all values in the range are non-null
      return additional;
    } else {
      // previous must be null, return the new constraint
      return additional;
    }
  }

  @SuppressWarnings("unchecked")
  private void checkContained(String name, Object... values) {
    for (Object value : values) {
      if (constraints.containsKey(name)) {
        Predicate current = constraints.get(name);
        Preconditions.checkArgument(current.apply(value),
            "%s does not match %s", current, value);
      }
    }
  }

  private static Object get(Object entity, String name) {
    if (entity instanceof GenericRecord) {
      return ((GenericRecord) entity).get(name);
    } else {
      try {
        PropertyDescriptor propertyDescriptor = new PropertyDescriptor(name,
            entity.getClass(), getter(name), null /* assume read only */);
        return propertyDescriptor.getReadMethod().invoke(entity);
      } catch (IllegalAccessException e) {
        throw new IllegalStateException("Cannot read property " + name +
            " from " + entity, e);
      } catch (InvocationTargetException e) {
        throw new IllegalStateException("Cannot read property " + name +
            " from " + entity, e);
      } catch (IntrospectionException e) {
        throw new IllegalStateException("Cannot read property " + name +
            " from " + entity, e);
      }
    }
  }

  private static String getter(String name) {
    return "get" +
        name.substring(0, 1).toUpperCase(Locale.ENGLISH) +
        name.substring(1);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).addValue(constraints).toString();
  }
}
