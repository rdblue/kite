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

package org.kitesdk.data.partition;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.kitesdk.data.FieldPartitioner;
import org.kitesdk.data.TestHelpers;
import org.kitesdk.data.spi.Predicates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestPartitionerProjection {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestPartitionerProjection.class);

  public long sepInstant = 1379020547042l; // Thu Sep 12 14:15:47 PDT 2013
  public long octInstant = 1381612547042l; // Sat Oct 12 14:15:47 PDT 2013
  public long novInstant = 1384204547042l; // Mon Nov 11 13:15:47 PST 2013
  public static final long ONE_DAY_MILLIS = 86400000; // 24 * 60 * 60 * 1000
  public static final long ONE_YEAR_MILLIS = ONE_DAY_MILLIS * 365;

  @Test
  public void testDateFormatPartitionerRangePredicate() {
    FieldPartitioner<Long, String> fp =
        new DateFormatPartitioner("timestamp", "date", "yyyy-MM-dd");

    Predicate<String> projected = fp.project(
        Ranges.open(octInstant, octInstant + ONE_DAY_MILLIS));
    Assert.assertEquals(Ranges.closed("2013-10-12", "2013-10-13"), projected);
  }

  @Test
  @Ignore // Not yet implemented
  public void testDateFormatPartitionerSatisfied() {
    FieldPartitioner<Long, String> fp =
        new DateFormatPartitioner("timestamp", "date", "yyyy-MM-dd");
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    format.setTimeZone(TimeZone.getTimeZone("UTC"));

    Predicate<String> projected = fp.projectSatisfied(
        Ranges.open(sepInstant, novInstant));
    Assert.assertEquals(Ranges.closed("2013-09-13", "2013-11-10"), projected);
  }

  @Test
  public void testDateFormatPartitionerSetPredicate() {
    FieldPartitioner<Long, String> fp =
        new DateFormatPartitioner("timestamp", "date", "yyyy-MM-dd");
    Assert.assertEquals(
        Predicates.in("2013-09-12", "2013-10-12", "2013-11-11"),
        fp.project(Predicates.in(sepInstant, octInstant, novInstant)));
    Assert.assertNull(fp.projectSatisfied(
        Predicates.in(sepInstant, octInstant, novInstant)));
  }

  @Test
  public void testYearFieldPartitionerRangePredicate() {
    FieldPartitioner<Long, Integer> fp =
        new YearFieldPartitioner("timestamp", "year");
    // Range within a year
    Assert.assertEquals(Ranges.singleton(2013),
        fp.project(Ranges.open(sepInstant, novInstant)));
    Assert.assertNull("No year value definitely satisfies original predicate",
        fp.projectSatisfied(Ranges.open(sepInstant, novInstant)));

    // Range spanning a year
    Assert.assertEquals(Ranges.closed(2012, 2013), fp.project(
        Ranges.open(sepInstant - ONE_YEAR_MILLIS, novInstant)));
    Assert.assertNull("No year value definitely satisfies original predicate",
        fp.projectSatisfied(Ranges.open(
            sepInstant - ONE_YEAR_MILLIS, novInstant)));

    // Range spanning two years
    Assert.assertEquals(Ranges.closed(2012, 2014), fp.project(Ranges.open(
        sepInstant - ONE_YEAR_MILLIS, novInstant + ONE_YEAR_MILLIS)));
    Assert.assertEquals(Ranges.singleton(2013), fp.projectSatisfied(Ranges.open(
        sepInstant - ONE_YEAR_MILLIS, novInstant + ONE_YEAR_MILLIS)));

    // open ended ranges
    Assert.assertEquals(Ranges.atLeast(2013),
        fp.project(Ranges.greaterThan(sepInstant)));
    Assert.assertEquals(Ranges.atLeast(2014),
        fp.projectSatisfied(Ranges.greaterThan(sepInstant)));
    Assert.assertEquals(Ranges.atMost(2013),
        fp.project(Ranges.atMost(sepInstant)));
    Assert.assertEquals(Ranges.atMost(2012),
        fp.projectSatisfied(Ranges.atMost(sepInstant)));

    // edge cases
    long first2013 = new DateTime(2013, 1, 1, 0, 0, DateTimeZone.UTC)
        .getMillis();
    long last2012 = first2013 - 1;
    Assert.assertEquals(Ranges.atMost(2012),
        fp.projectSatisfied(Ranges.atMost(last2012)));
    Assert.assertEquals(Ranges.atMost(2012),
        fp.projectSatisfied(Ranges.lessThan(first2013)));
    Assert.assertEquals(Ranges.atLeast(2013),
        fp.projectSatisfied(Ranges.atLeast(first2013)));
    Assert.assertEquals(Ranges.atLeast(2013),
        fp.projectSatisfied(Ranges.greaterThan(last2012)));
  }

  @Test
  public void testYearFieldPartitionerSetPredicate() {
    FieldPartitioner<Long, Integer> fp =
        new YearFieldPartitioner("timestamp", "year");

    // A single year
    Assert.assertEquals(Predicates.in(2013),
        fp.project(Predicates.in(sepInstant, octInstant)));
    Assert.assertNull(fp.projectSatisfied(Predicates.in(sepInstant)));

    // Multiple years
    Assert.assertEquals(Predicates.in(2012, 2013),
        fp.project(Predicates.in(sepInstant - ONE_YEAR_MILLIS, octInstant)));
    Assert.assertNull(fp.projectSatisfied(
        Predicates.in(sepInstant - ONE_YEAR_MILLIS, octInstant)));
  }

  @Test
  public void testCalendarFieldPartitioners() {
    List<CalendarFieldPartitioner> fps = Lists.newArrayList(
        new MonthFieldPartitioner("timestamp", "month"),
        new DayOfMonthFieldPartitioner("timestamp", "day"),
        new HourFieldPartitioner("timestamp", "hour"),
        new MinuteFieldPartitioner("timestamp", "min"));
    // none of these fields can produce a valid predicate independently
    for (CalendarFieldPartitioner fp : fps) {
      Assert.assertNull(fp.project(Predicates.in(octInstant)));
      Assert.assertNull(fp.projectSatisfied(Predicates.in(octInstant)));
      Assert.assertNull(fp.project(Ranges.greaterThan(sepInstant)));
      Assert.assertNull(fp.projectSatisfied(Ranges.open(octInstant, novInstant)));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testHashFieldPartitionerRangePredicate() {
    FieldPartitioner<Object, Integer> fp = new HashFieldPartitioner("name", 50);
    // cannot enumerate all inputs, so we can't calculate the set of potential
    // hash values other than all hash values mod the number of buckets
    Assert.assertNull(fp.project((Predicate)Ranges.open("a", "b")));
    Assert.assertNull(fp.projectSatisfied((Predicate) Ranges.open("a", "b")));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testHashFieldPartitionerSetPredicate() {
    FieldPartitioner<Object, Integer> fp = new HashFieldPartitioner("name", 50);
    Assert.assertEquals(Predicates.in(fp.apply("a"), fp.apply("b")),
        fp.project((Predicate)Predicates.in("a", "b")));
    // the set of inputs that result in a particular value is not closed
    Assert.assertNull(fp.projectSatisfied((Predicate)Predicates.in("a")));
  }

  @Test
  public void testIdentityFieldPartitionerRangePredicate() {
    FieldPartitioner<String, String> fp =
        new IdentityFieldPartitioner<String>("str", String.class, 50);
    Range<String> r = Ranges.openClosed("a", "b");
    Assert.assertEquals(r, fp.project(r));
    Assert.assertEquals(r, fp.projectSatisfied(r));
  }

  @Test
  public void testIdentityFieldPartitionerSetPredicate() {
    FieldPartitioner<String, String> fp =
        new IdentityFieldPartitioner<String>("str", String.class, 50);
    Predicates.In<String> s = Predicates.in("a", "b");
    Assert.assertEquals(s, fp.project(s));
    Assert.assertEquals(s, fp.projectSatisfied(s));
  }

  @Test
  public void testIntRangeFieldPartitionerRangePredicate() {
    final FieldPartitioner<Integer, Integer> fp =
        new IntRangeFieldPartitioner("num", 5, 10, 15, 20);
    Assert.assertEquals(Ranges.closed(1, 2), fp.project(Ranges.open(5, 15)));
    Assert.assertEquals(Ranges.closed(0, 2), fp.project(Ranges.open(4, 15)));

    // even though 21 is above the last bound, the range is valid if open
    Assert.assertEquals(Ranges.closed(0, 3), fp.project(Ranges.open(4, 21)));
    TestHelpers.assertThrows("Should not project an invalid range",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        fp.project(Ranges.openClosed(5, 21));
      }
    });

    Assert.assertEquals(Ranges.closed(1, 2),
        fp.projectSatisfied(Ranges.open(5, 15)));
    Assert.assertEquals(Ranges.singleton(1),
        fp.projectSatisfied(Ranges.open(5, 14)));
    Assert.assertEquals(Ranges.atMost(2),
        fp.projectSatisfied(Ranges.atMost(15)));
    Assert.assertEquals(Ranges.atMost(3),
        fp.projectSatisfied(Ranges.lessThan(21)));

    Assert.assertNull(fp.projectSatisfied(Ranges.closed(15, 16)));

    // unbounded range is no problem, although accepted values would be
    // rejected if partitioned
    Assert.assertEquals(Ranges.atLeast(3),
        fp.projectSatisfied(Ranges.atLeast(14)));

    TestHelpers.assertThrows("Should not project an invalid range",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        fp.projectSatisfied(Ranges.openClosed(5, 21));
      }
    });

  }

  @Test
  public void testIntRangeFieldPartitionerSetPredicate() {
    final FieldPartitioner<Integer, Integer> fp =
        new IntRangeFieldPartitioner("num", 5, 10, 15, 20);
    Assert.assertEquals(Predicates.in(1, 3),
        fp.project(Predicates.in(6, 7, 16, 17)));
    TestHelpers.assertThrows("Should not project invalid set",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        fp.project(Predicates.in(21));
      }
    });

    // null if no full range is included
    Assert.assertNull(fp.projectSatisfied(Predicates.in(6, 7, 16, 17)));
    Assert.assertEquals(Predicates.in(1),
        fp.projectSatisfied(Predicates.in(6, 7, 8, 9, 10, 16, 17)));
    Assert.assertEquals(Predicates.in(1, 3),
        fp.projectSatisfied(Predicates.in(
            5, 6, 7, 8, 9, 10, 16, 17, 18, 19, 20)));

    // doesn't complain about values that are too large
    Assert.assertEquals(Predicates.in(1),
        fp.projectSatisfied(Predicates.in(6, 7, 8, 9, 10, 16, 17, 22)));
  }

  @Test
  public void testRangeFieldPartitionerRangePredicate() {
    final FieldPartitioner<String, String> fp =
        new RangeFieldPartitioner("str", "a", "b", "c");
    // projected to sets because the range ["a", "b"] includes "aa", etc.
    Assert.assertEquals(Predicates.in("a"),
        fp.project(Ranges.atMost("a")));
    Assert.assertEquals(Predicates.in("a", "b"),
        fp.project(Ranges.closedOpen("a", "b")));
    Assert.assertEquals(Predicates.in("a", "b"),
        fp.project(Ranges.closedOpen("a", "aa")));
    Assert.assertEquals(Predicates.in("a", "b", "c"),
        fp.project(Ranges.closedOpen("a", "ba")));
    Assert.assertEquals(Predicates.in("a", "b", "c"),
        fp.project(Ranges.closedOpen("0", "c")));
    Assert.assertEquals(Predicates.in("c"),
        fp.project(Ranges.atLeast("c")));

    TestHelpers.assertThrows("Cannot project endpoint outside of bounds",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        fp.project(Ranges.atMost("cc"));
      }
    });

    Assert.assertNull(fp.projectSatisfied(Ranges.lessThan("a")));
    Assert.assertEquals(Predicates.in("a"),
        fp.projectSatisfied(Ranges.atMost("a")));
    Assert.assertEquals(Predicates.in("a"),
        fp.projectSatisfied(Ranges.lessThan("b")));
    Assert.assertEquals(Predicates.in("a", "b"),
        fp.projectSatisfied(Ranges.atMost("b")));
    Assert.assertEquals(Predicates.in("c"),
        fp.projectSatisfied(Ranges.atLeast("b")));
    Assert.assertEquals(Predicates.in("c"),
        fp.projectSatisfied(Ranges.greaterThan("b")));
    Assert.assertEquals(Predicates.in("a"),
        fp.projectSatisfied(Ranges.atMost("ab")));
  }

  @Test
  public void testRangeFieldPartitionerSetPredicate() {
    final FieldPartitioner<String, String> fp =
        new RangeFieldPartitioner("str", "a", "b", "c");
    Assert.assertEquals(Predicates.in("a"), fp.project(Predicates.in("0")));
    Assert.assertEquals(Predicates.in("a"), fp.project(Predicates.in("a")));
    Assert.assertEquals(Predicates.in("b"), fp.project(Predicates.in("aa")));
    Assert.assertEquals(Predicates.in("a", "b"),
        fp.project(Predicates.in("a", "aa", "b")));

    TestHelpers.assertThrows("Cannot project endpoint outside of bounds",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        fp.project(Predicates.in("cc"));
      }
    });

    // cannot enumerate all of the potential input, so no satisfied projection
    Assert.assertNull(fp.projectSatisfied(Predicates.in("0")));
    Assert.assertNull(fp.projectSatisfied(Predicates.in("a")));
    Assert.assertNull(fp.projectSatisfied(Predicates.in("aa")));
    Assert.assertNull(fp.projectSatisfied(Predicates.in("a", "aa", "b")));
    Assert.assertNull(fp.projectSatisfied(Predicates.in("cc")));
  }

  @Test
  public void testListFieldPartitionerRangePredicate() {

  }

  @Test
  public void testListFieldPartitionerSetPredicate() {

  }
}
