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
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import java.util.AbstractMap;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.kitesdk.data.FieldPartitioner;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.partition.CalendarFieldPartitioner;

@Immutable
public class TimeDomain {
  private static final int[] order = new int[]{
      Calendar.YEAR, Calendar.MONTH, Calendar.DAY_OF_MONTH,
      Calendar.HOUR_OF_DAY, Calendar.MINUTE, Calendar.SECOND
  };

  private static final
  LoadingCache<Map.Entry<PartitionStrategy, String>, TimeDomain> domains =
      CacheBuilder.newBuilder().build(
          new CacheLoader<Map.Entry<PartitionStrategy, String>, TimeDomain>() {
        @Override
        public TimeDomain load(Map.Entry<PartitionStrategy, String> entry) {
          return new TimeDomain(entry.getKey(), entry.getValue());
        }
      });

  public static TimeDomain get(PartitionStrategy strategy, String source) {
    return domains.getUnchecked(new AbstractMap
        .SimpleImmutableEntry<PartitionStrategy, String>(strategy, source));
  }

  private final List<CalendarFieldPartitioner> partitioners;

  public TimeDomain(PartitionStrategy strategy, String sourceName) {
    Map<Integer, CalendarFieldPartitioner> mapping = Maps.newHashMap();
    for (FieldPartitioner fp : strategy.getFieldPartitioners()) {
      // there may be partitioners for more than one source field
      if (sourceName.equals(fp.getSourceName()) &&
          fp instanceof CalendarFieldPartitioner) {
        mapping.put(
            ((CalendarFieldPartitioner) fp).getCalendarField(),
            (CalendarFieldPartitioner) fp);
      }
    }
    // get the partitioners to check for this strategy
    this.partitioners = Lists.newArrayList();
    for (int field : order) {
      // if there is no partition for the next field, then all are included
      // example: yyyy/mm/dd partitioning accepts when field is hour
      if (!mapping.containsKey(field)) {
        break;
      }
      partitioners.add(mapping.get(field));
    }
  }

  public Predicate<StorageKey> project(Predicate<Long> predicate) {
    if (predicate instanceof Constraints.In) {
      return new TimeSetPredicate((Constraints.In<Long>) predicate);
    } else if (predicate instanceof Range) {
      return new TimeRangePredicate((Range<Long>) predicate);
    } else {
      return null;
    }
  }

  private class TimeSetPredicate implements Predicate<StorageKey> {
    private final Constraints.In<List<Integer>> times;

    private TimeSetPredicate(Constraints.In<Long> times) {
      this.times = times.transform(new Function<Long, List<Integer>>() {
        @Override
        public List<Integer> apply(@Nullable Long timestamp) {
          List<Integer> time = Lists
              .newArrayListWithExpectedSize(partitioners.size());
          for (CalendarFieldPartitioner fp : partitioners) {
            time.add(fp.apply(timestamp));
          }
          return time;
        }
      });
    }

    @Override
    public boolean apply(@Nullable StorageKey key) {
      List<Integer> time = Lists
          .newArrayListWithExpectedSize(partitioners.size());
      for (CalendarFieldPartitioner fp : partitioners) {
        time.add((Integer) key.get(fp.getName()));
      }
      return times.apply(time);
    }
  }

  private class TimeRangePredicate implements Predicate<StorageKey> {
    private final Range<Long> timeRange;

    private TimeRangePredicate(Range<Long> timeRange) {
      this.timeRange = timeRange;
    }

    @Override
    public boolean apply(@Nullable StorageKey key) {
      Preconditions.checkNotNull(key);
      for (CalendarFieldPartitioner calField : partitioners) {
        int value = (Integer) key.get(calField.getName());
        int lower = calField.apply(timeRange.lowerEndpoint());
        int upper = calField.apply(timeRange.upperEndpoint());
        if (lower < value) {
          if (value < upper) {
            // strictly within range, so all other levels must be
            // example: 2013-4-10 to 2013-10-4 => 4 < month < 10 => accept
            return true;
          } else if (value > upper) {
            // falls out of the range at this level
            return false;
          }
        } else if (value < lower) {
          // falls out of the range at this level
          return false;
        }
        // value was equal to one endpoint, continue checking
      }
      // each position was satisfied, so the key matches
      return true;
    }
  }
}
