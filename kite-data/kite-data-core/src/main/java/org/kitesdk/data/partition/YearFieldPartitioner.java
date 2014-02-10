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

import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.DiscreteDomains;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;
import java.util.Calendar;
import org.kitesdk.data.spi.Predicates;

@Beta
@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value="SE_COMPARATOR_SHOULD_BE_SERIALIZABLE",
    justification="Implement if we intend to use in Serializable objects "
        + " (e.g., TreeMaps) and use java serialization.")
public class YearFieldPartitioner extends CalendarFieldPartitioner {
  public YearFieldPartitioner(String sourceName, String name) {
    super(sourceName, name, Calendar.YEAR, 5); // arbitrary number of partitions
  }

  @Override
  public Predicate<Integer> project(Predicate<Long> predicate) {
    // year is the only time field that can be projected
    if (predicate instanceof Predicates.Exists) {
      return Predicates.exists();
    } else if (predicate instanceof Predicates.In) {
      return ((Predicates.In<Long>) predicate).transform(this);
    } else if (predicate instanceof Range) {
      return Predicates.transformClosed(
          Predicates.adjustClosed(
              (Range<Long>) predicate, DiscreteDomains.longs()),
          this);
    } else {
      return null;
    }
  }

  public <S extends Comparable, T extends Comparable<T>>
  Range<T> excludeEnds(Range<S> range, Function<S, T> func,
                       DiscreteDomain<S> domain,
                       DiscreteDomain<T> imageDomain) {
    Range<S> adjusted = Predicates.adjustClosed(range, domain);
    if (adjusted.hasLowerBound()) {
      S lower = adjusted.lowerEndpoint();
      T lowerImage = func.apply(lower);
      if (lowerImage.equals(func.apply(domain.previous(lower)))) {
        // at least one excluded value maps to the same value
        lowerImage = imageDomain.next(lowerImage);
      }
      if (adjusted.hasUpperBound()) {
        S upper = adjusted.upperEndpoint();
        T upperImage = func.apply(upper);
        if (upperImage.equals(func.apply(domain.next(upper)))) {
          // at least one excluded value maps to the same value
          upperImage = imageDomain.previous(upperImage);
        }
        if (lowerImage.compareTo(upperImage) <= 0) {
          return Ranges.closed(lowerImage, upperImage);
        }
      } else {
        return Ranges.atLeast(lowerImage);
      }
    } else if (adjusted.hasUpperBound()) {
      S upper = adjusted.upperEndpoint();
      T upperImage = func.apply(upper);
      if (upperImage.equals(func.apply(domain.next(upper)))) {
        // at least one excluded value maps to the same value
        upperImage = imageDomain.previous(upperImage);
      }
      return Ranges.atMost(upperImage);
    }
    return null;
  }

  @Override
  public Predicate<Integer> projectSatisfied(Predicate<Long> predicate) {
    if (predicate instanceof Predicates.Exists) {
      return Predicates.exists();
    } else if (predicate instanceof Predicates.In) {
      // not enough information to make a judgement on behalf of the
      // original predicate. the year may match when month does not
      return null;
    } else if (predicate instanceof Range) {
      //return Predicates.transformClosedConservative(
      //    (Range<Long>) predicate, this, DiscreteDomains.integers());
      Range<Long> adjusted = Predicates.adjustClosed(
          (Range<Long>) predicate, DiscreteDomains.longs());
      if (adjusted.hasLowerBound()) {
        long lower = adjusted.lowerEndpoint();
        int lowerImage = apply(lower);
        if (apply(lower - 1) == lowerImage) {
          // at least one excluded value maps to the same value
          lowerImage += 1;
        }
        if (adjusted.hasUpperBound()) {
          long upper = adjusted.upperEndpoint();
          int upperImage = apply(upper);
          if (apply(upper + 1) == upperImage) {
            // at least one excluded value maps to the same value
            upperImage -= 1;
          }
          if (lowerImage <= upperImage) {
            return Ranges.closed(lowerImage, upperImage);
          }
        } else {
          return Ranges.atLeast(lowerImage);
        }
      } else if (adjusted.hasUpperBound()) {
        long upper = adjusted.upperEndpoint();
        int upperImage = apply(upper);
        if (apply(upper + 1) == upperImage) {
          // at least one excluded value maps to the same value
          upperImage -= 1;
        }
        return Ranges.atMost(upperImage);
      }
    }
    // could not produce a satisfying predicate
    return null;
  }
}
