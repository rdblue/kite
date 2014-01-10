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

import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.TestHelpers;

import static org.junit.Assert.assertEquals;

public class TestMarkerRange {

  private static Marker OCT_12;
  private static Marker OCT_15;
  private static Marker SEPT_30;
  private static Marker NOV_1;
  private static PartitionStrategy strategy;
  private static MarkerComparator comparator;

  @BeforeClass
  public static void setup() {
    OCT_12 = new Marker.Builder()
        .add("year", 2013)
        .add("month", 10)
        .add("day", 12)
        .build();
    OCT_15 = new Marker.Builder()
        .add("year", 2013)
        .add("month", 10)
        .add("day", 15)
        .build();
    SEPT_30 = new Marker.Builder()
        .add("year", 2013)
        .add("month", 9)
        .add("day", 30)
        .build();
    NOV_1 = new Marker.Builder()
        .add("year", 2013)
        .add("month", 11)
        .add("day", 1)
        .build();
    strategy = new PartitionStrategy.Builder()
        .year("timestamp")
        .month("timestamp")
        .day("timestamp")
        .build();
    comparator = new MarkerComparator(strategy);
  }

  @Test
  public void testCombine() {
    final MarkerRange unbounded = new MarkerRange(comparator);
    final MarkerRange unboundedToNov1 = new MarkerRange(comparator).to(NOV_1);
    final MarkerRange oct12ToOct15 = new MarkerRange(comparator).from(OCT_12).to(OCT_15);
    final MarkerRange sept30ToOct15 = new MarkerRange(comparator).from(SEPT_30).to(OCT_15);
    final MarkerRange oct12ToNov1 = new MarkerRange(comparator).from(OCT_12).to(NOV_1);
    final MarkerRange sept30ToNov1 = new MarkerRange(comparator).from(SEPT_30).to(NOV_1);
    final MarkerRange sept30ToUnbounded = new MarkerRange(comparator).from(SEPT_30);

    // Combine with self
    assertEquals(unbounded, unbounded.combine(unbounded));
    assertEquals(oct12ToOct15, oct12ToOct15.combine(oct12ToOct15));
    assertEquals(unboundedToNov1, unboundedToNov1.combine(unboundedToNov1));
    assertEquals(sept30ToUnbounded, sept30ToUnbounded.combine(sept30ToUnbounded));

    // Combine with double unbounded
    assertEquals(oct12ToOct15, unbounded.combine(oct12ToOct15));
    assertEquals(oct12ToOct15, oct12ToOct15.combine(unbounded));

    // Combine with single unbounded
    assertEquals(oct12ToOct15, unboundedToNov1.combine(oct12ToOct15));
    assertEquals(oct12ToOct15, sept30ToUnbounded.combine(oct12ToOct15));

    assertEquals(oct12ToOct15, sept30ToNov1.combine(oct12ToOct15));
    TestHelpers.assertThrows("Can't combine with bounds outside range.",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        oct12ToOct15.combine(sept30ToNov1);
      }
    });

    TestHelpers.assertThrows("Can't combine with bounds outside range.",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        sept30ToOct15.combine(oct12ToNov1);
      }
    });
    TestHelpers.assertThrows("Can't combine with bounds outside range.",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        oct12ToNov1.combine(sept30ToOct15);
      }
    });
  }
}
