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

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.kitesdk.data.*;
import org.kitesdk.data.event.StandardEvent;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public abstract class TestRefinableViews extends MiniDFSTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRefinableViews.class);

  protected static final long now = System.currentTimeMillis();
  protected static final StandardEvent event = StandardEvent.newBuilder()
      .setEventInitiator("TestRefinableViews")
      .setEventName("TestEvent")
      .setUserId(0)
      .setSessionId("session-0")
      .setIp("localhost")
      .setTimestamp(now + 35405168l)
      .build();
  protected static final StandardEvent sepEvent = StandardEvent
      .newBuilder(event)
      .setTimestamp(1379020547042l) // Thu Sep 12 14:15:47 PDT 2013
      .build();
  protected static final StandardEvent octEvent = StandardEvent
      .newBuilder(event)
      .setTimestamp(1381612547042l) // Sat Oct 12 14:15:47 PDT 2013
      .build();
  protected static final StandardEvent novEvent = StandardEvent
      .newBuilder(event)
      .setTimestamp(1384204547042l) // Mon Nov 11 13:15:47 PST 2013
      .build();

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][] {
        { false },  // default to local FS
        { true } }; // default to distributed FS
    return Arrays.asList(data);
  }

  // whether this should use the DFS
  private final boolean distributed;

  protected TestRefinableViews(boolean distributed) {
    this.distributed = distributed;
  }

  // from subclasses
  protected DatasetRepository repo = null;

  public abstract DatasetRepository newRepo();

  protected Configuration conf = null;
  protected FileSystem fs;
  protected PartitionStrategy strategy = null;
  protected DatasetDescriptor testDescriptor = null;
  protected RefineableView<StandardEvent> unbounded = null;

  @Before
  public void setup() throws Exception {
    this.conf = (distributed ?
        MiniDFSTest.getConfiguration() :
        new Configuration());
    this.fs = FileSystem.get(conf);

    this.repo = newRepo();
    this.strategy = new PartitionStrategy.Builder()
        .year("timestamp")
        .month("timestamp")
        .day("timestamp")
        .build();
    this.testDescriptor = new DatasetDescriptor.Builder()
        .schemaUri("resource:standard_event.avsc")
        .partitionStrategy(strategy)
        .build();
    this.unbounded = repo.create("test", testDescriptor);
  }

  @Test public abstract void testCoveringPartitions();

  public static <E> void assertContentEquals(Set<E> expected, View<E> view) {
    DatasetReader<E> reader = view.newReader();
    try {
      reader.open();
      Assert.assertEquals(expected,
          Sets.newHashSet((Iterable<E>) reader));
    } finally {
      reader.close();
    }
  }

  @Test
  public void testLimitedReader() {
    // NOTE: this is an un-restricted write so all should succeed
    DatasetWriter<StandardEvent> writer = unbounded.newWriter();
    try {
      writer.open();
      writer.write(sepEvent);
      writer.write(octEvent);
      writer.write(novEvent);
    } finally {
      writer.close();
    }

    // unbounded
    assertContentEquals(Sets.newHashSet(sepEvent, octEvent, novEvent),
        unbounded);

    long octInstant = octEvent.getTimestamp();

    // single bound
    assertContentEquals(Sets.newHashSet(octEvent),
        unbounded.with("timestamp", octInstant));
    assertContentEquals(Sets.newHashSet(octEvent, novEvent),
        unbounded.from("timestamp", octInstant));
    assertContentEquals(Sets.newHashSet(sepEvent, octEvent),
        unbounded.to("timestamp", octInstant));
    // these two test that the constraints are applied after the partitions are
    // filtered. the partition matches, but the events do not
    assertContentEquals(Sets.newHashSet(novEvent),
        unbounded.fromAfter("timestamp", octInstant));
    assertContentEquals(Sets.newHashSet(sepEvent),
        unbounded.toBefore("timestamp", octInstant));

    // in range
    long octStart = new DateTime(2013, 10, 1, 0, 0, DateTimeZone.UTC).getMillis();
    long novStart = new DateTime(2013, 11, 1, 0, 0, DateTimeZone.UTC).getMillis();
    assertContentEquals(Sets.<StandardEvent>newHashSet(octEvent),
        unbounded.from("timestamp", octStart).toBefore("timestamp", novStart));
  }

  @Test
  public void testLimitedWriter() {
    long start = new DateTime(2013, 10, 1, 0, 0, DateTimeZone.UTC).getMillis();
    long end = new DateTime(2013, 11, 14, 0, 0, DateTimeZone.UTC).getMillis();
    final RefineableView<StandardEvent> range = unbounded
        .from("timestamp", start).toBefore("timestamp", end);

    DatasetWriter<StandardEvent> writer = range.newWriter();
    try {
      writer.open();
      writer.write(octEvent);
      writer.write(novEvent);
    } finally {
      writer.close();
    }
    assertContentEquals(Sets.newHashSet(octEvent, novEvent), range);

    TestHelpers.assertThrows("Should reject older event",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        DatasetWriter<StandardEvent> writer = range.newWriter();
        try {
          writer.open();
          writer.write(sepEvent);
        } finally {
          writer.close();
        }
      }
    });
    TestHelpers.assertThrows("Should reject current event",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        DatasetWriter<StandardEvent> writer = range.newWriter();
        try {
          writer.open();
          writer.write(event);
        } finally {
          writer.close();
        }
      }
    });
  }

  @Test
  public void testFromView() {
    long instant = new DateTime(2013, 10, 1, 0, 0, DateTimeZone.UTC).getMillis();
    long later = instant + 1;
    final long earlier = instant - 1;
    final RefineableView<StandardEvent> fromOctober =
        unbounded.from("timestamp", instant);

    // test events
    Assert.assertFalse("Should not contain older event",
        fromOctober.canContain(sepEvent));
    Assert.assertTrue("Should contain event",
        fromOctober.canContain(octEvent));
    Assert.assertTrue("Should contain newer event",
        fromOctober.canContain(novEvent));
    Assert.assertTrue("Should contain current event",
        fromOctober.canContain(event));

    // special case: to(same instant)
    Assert.assertNotNull("to(same instant) should succeed",
        fromOctober.to("timestamp", instant));

    // test limiting with other view methods
    Assert.assertNotNull("adding independent constraints should succeed",
        fromOctober.with("event_name", "Event"));
    Assert.assertNotNull("with(later instant) should succeed",
        fromOctober.with("timestamp", later));
    Assert.assertNotNull("from(later instant) should succeed",
        fromOctober.from("timestamp", later));
    Assert.assertNotNull("fromAfter(later instant) should succeed",
        fromOctober.fromAfter("timestamp", later));
    Assert.assertNotNull("to(later instant) should succeed",
        fromOctober.to("timestamp", later));
    Assert.assertNotNull("toAfter(later instant) should succeed",
        fromOctober.toBefore("timestamp", later));

    // should fail when constraints produce an empty set
    TestHelpers.assertThrows("with(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        fromOctober.with("timestamp", earlier);
      }
    });
    TestHelpers.assertThrows("to(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        fromOctober.to("timestamp", earlier);
      }
    });
    TestHelpers.assertThrows("toBefore(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        fromOctober.toBefore("timestamp", earlier);
      }
    });

    // this behavior is debatable, but something odd is happening
    TestHelpers.assertThrows("from(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        fromOctober.from("timestamp", earlier);
      }
    });
    TestHelpers.assertThrows("fromAfter(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        fromOctober.fromAfter("timestamp", earlier);
      }
    });
  }

  @Test
  public void testFromAfterView() {
    final long instant = new DateTime(2013, 9, 30, 12, 59, 59, 999, DateTimeZone.UTC).getMillis();
    long later = instant + 1;
    final long earlier = instant - 1;
    final RefineableView<StandardEvent> afterSep =
        unbounded.fromAfter("timestamp", instant);

    // test events
    Assert.assertFalse("Should not contain older event",
        afterSep.canContain(sepEvent));
    Assert.assertTrue("Should contain event",
        afterSep.canContain(octEvent));
    Assert.assertTrue("Should contain newer event",
        afterSep.canContain(novEvent));
    Assert.assertTrue("Should contain current event",
        afterSep.canContain(event));

    // special case: to(same instant)
    TestHelpers.assertThrows("to(same instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        afterSep.to("timestamp", instant);
      }
    });

    // test limiting with other view methods
    Assert.assertNotNull("adding independent constraints should succeed",
        afterSep.with("event_name", "Event"));
    Assert.assertNotNull("with(later instant) should succeed",
        afterSep.with("timestamp", later));
    Assert.assertNotNull("from(later instant) should succeed",
        afterSep.from("timestamp", later));
    Assert.assertNotNull("fromAfter(later instant) should succeed",
        afterSep.fromAfter("timestamp", later));
    Assert.assertNotNull("to(later instant) should succeed",
        afterSep.to("timestamp", later));
    Assert.assertNotNull("toAfter(later instant) should succeed",
        afterSep.toBefore("timestamp", later));

    // should fail when constraints produce an empty set
    TestHelpers.assertThrows("with(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        afterSep.with("timestamp", earlier);
      }
    });
    TestHelpers.assertThrows("to(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        afterSep.to("timestamp", earlier);
      }
    });
    TestHelpers.assertThrows("toBefore(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        afterSep.toBefore("timestamp", earlier);
      }
    });

    // this behavior is debatable, but something odd is happening
    TestHelpers.assertThrows("from(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        afterSep.from("timestamp", earlier);
      }
    });
    TestHelpers.assertThrows("fromAfter(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        afterSep.fromAfter("timestamp", earlier);
      }
    });
  }

  @Test
  public void testToView() {
    final long instant = new DateTime(2013, 9, 30, 12, 59, 59, 999, DateTimeZone.UTC).getMillis();
    final long later = instant + 1;
    long earlier = instant - 1;
    final RefineableView<StandardEvent> toOct =
        unbounded.to("timestamp", instant);

    // test events
    Assert.assertTrue("Should contain older event",
        toOct.canContain(sepEvent));
    Assert.assertFalse("Should not contain event",
        toOct.canContain(octEvent));
    Assert.assertFalse("Should not contain newer event",
        toOct.canContain(novEvent));
    Assert.assertFalse("Should not contain current event",
        toOct.canContain(event));

    // special case: from(same instant)
    Assert.assertNotNull("from(same instant) should succeed",
        toOct.from("timestamp", instant));

    // test limiting with other view methods
    Assert.assertNotNull("adding independent constraints should succeed",
        toOct.with("event_name", "Event"));
    Assert.assertNotNull("with(earlier instant) should succeed",
        toOct.with("timestamp", earlier));
    Assert.assertNotNull("from(earlier instant) should succeed",
        toOct.from("timestamp", earlier));
    Assert.assertNotNull("fromAfter(earlier instant) should succeed",
        toOct.fromAfter("timestamp", earlier));
    Assert.assertNotNull("to(earlier instant) should succeed",
        toOct.to("timestamp", earlier));
    Assert.assertNotNull("toAfter(earlier instant) should succeed",
        toOct.toBefore("timestamp", earlier));

    // should fail when constraints produce an empty set
    TestHelpers.assertThrows("with(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        toOct.with("timestamp", later);
      }
    });
    TestHelpers.assertThrows("to(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        toOct.to("timestamp", later);
      }
    });
    TestHelpers.assertThrows("toBefore(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        toOct.toBefore("timestamp", later);
      }
    });

    // this behavior is debatable, but something odd is happening
    TestHelpers.assertThrows("from(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        toOct.from("timestamp", later);
      }
    });
    TestHelpers.assertThrows("fromAfter(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        toOct.fromAfter("timestamp", later);
      }
    });
  }

  @Test
  public void testToBeforeView() {
    final long instant = new DateTime(2013, 10, 1, 0, 0, DateTimeZone.UTC).getMillis();
    final long later = instant + 1;
    long earlier = instant - 1;
    final RefineableView<StandardEvent> beforeOct =
        unbounded.toBefore("timestamp", instant);

    // test events
    Assert.assertTrue("Should contain older event",
        beforeOct.canContain(sepEvent));
    Assert.assertFalse("Should not contain event",
        beforeOct.canContain(octEvent));
    Assert.assertFalse("Should not contain newer event",
        beforeOct.canContain(novEvent));
    Assert.assertFalse("Should not contain current event",
        beforeOct.canContain(event));

    // special case: from(same instant)
    TestHelpers.assertThrows("from(same instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        beforeOct.from("timestamp", instant);
      }
    });

    // test limiting with other view methods
    Assert.assertNotNull("adding independent constraints should succeed",
        beforeOct.with("event_name", "Event"));
    Assert.assertNotNull("with(earlier instant) should succeed",
        beforeOct.with("timestamp", earlier));
    Assert.assertNotNull("from(earlier instant) should succeed",
        beforeOct.from("timestamp", earlier));
    Assert.assertNotNull("fromAfter(earlier instant) should succeed",
        beforeOct.fromAfter("timestamp", earlier));
    Assert.assertNotNull("to(earlier instant) should succeed",
        beforeOct.to("timestamp", earlier));
    Assert.assertNotNull("toAfter(earlier instant) should succeed",
        beforeOct.toBefore("timestamp", earlier));

    // should fail when constraints produce an empty set
    TestHelpers.assertThrows("with(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        beforeOct.with("timestamp", later);
      }
    });
    TestHelpers.assertThrows("to(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        beforeOct.to("timestamp", later);
      }
    });
    TestHelpers.assertThrows("toBefore(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        beforeOct.toBefore("timestamp", later);
      }
    });

    // this behavior is debatable, but something odd is happening
    TestHelpers.assertThrows("from(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        beforeOct.from("timestamp", later);
      }
    });
    TestHelpers.assertThrows("fromAfter(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        beforeOct.fromAfter("timestamp", later);
      }
    });
  }

  @Test
  public void testWithView() {
    final long instant = new DateTime(2013, 10, 1, 0, 0, DateTimeZone.UTC).getMillis();
    final long later = instant + 1;
    final long earlier = instant - 1;
    final RefineableView<StandardEvent> withSpecificTimestamp =
        unbounded.with("timestamp", instant);

    Assert.assertNotNull("from(same instant) should succeed",
        withSpecificTimestamp.from("timestamp", instant));
    Assert.assertNotNull("to(same instant) should succeed",
        withSpecificTimestamp.to("timestamp", instant));
    Assert.assertNotNull("with(same instant) should succeed",
        withSpecificTimestamp.with("timestamp", instant));

    TestHelpers.assertThrows("with(different instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        withSpecificTimestamp.with("timestamp", later);
      }
    });
    TestHelpers.assertThrows("toBefore(same instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        withSpecificTimestamp.toBefore("timestamp", instant);
      }
    });
    TestHelpers.assertThrows("fromAfter(same instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        withSpecificTimestamp.fromAfter("timestamp", instant);
      }
    });
    TestHelpers.assertThrows("to(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        withSpecificTimestamp.to("timestamp", earlier);
      }
    });
    TestHelpers.assertThrows("from(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        withSpecificTimestamp.from("timestamp", later);
      }
    });
  }

  @Test
  public void testRange() {
    long start = new DateTime(2013, 10, 1, 0, 0, DateTimeZone.UTC).getMillis();
    long end = new DateTime(2013, 11, 1, 0, 0, DateTimeZone.UTC).getMillis();
    final long later = end + 1;
    final long earlier = start - 1;
    long included = octEvent.getTimestamp();
    final RefineableView<StandardEvent> oct = unbounded
        .from("timestamp", start).to("timestamp", end);

    // test events
    Assert.assertFalse("Should not contain older event",
        oct.canContain(sepEvent));
    Assert.assertTrue("Should contain event",
        oct.canContain(octEvent));
    Assert.assertFalse("Should not contain newer event",
        oct.canContain(novEvent));
    Assert.assertFalse("Should not contain current event",
        oct.canContain(event));

    // special cases
    Assert.assertNotNull("to(start) should succeed",
        oct.to("timestamp", start));
    Assert.assertNotNull("from(end) should succeed",
        oct.from("timestamp", end));

    // test limiting with other view methods
    Assert.assertNotNull("adding independent constraints should succeed",
        oct.with("event_name", "Event"));
    Assert.assertNotNull("with(included instant) should succeed",
        oct.with("timestamp", included));
    Assert.assertNotNull("from(included instant) should succeed",
        oct.from("timestamp", included));
    Assert.assertNotNull("fromAfter(included instant) should succeed",
        oct.fromAfter("timestamp", included));
    Assert.assertNotNull("to(included instant) should succeed",
        oct.to("timestamp", included));
    Assert.assertNotNull("toAfter(included instant) should succeed",
        oct.toBefore("timestamp", included));

    // should fail when constraints produce an empty set
    TestHelpers.assertThrows("with(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        oct.with("timestamp", earlier);
      }
    });
    TestHelpers.assertThrows("to(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        oct.to("timestamp", earlier);
      }
    });
    TestHelpers.assertThrows("toBefore(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        oct.toBefore("timestamp", earlier);
      }
    });

    // this behavior is debatable, but something odd is happening
    TestHelpers.assertThrows("from(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        oct.from("timestamp", earlier);
      }
    });
    TestHelpers.assertThrows("fromAfter(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        oct.fromAfter("timestamp", earlier);
      }
    });

    // should fail when constraints produce an empty set
    TestHelpers.assertThrows("with(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        oct.with("timestamp", later);
      }
    });
    TestHelpers.assertThrows("to(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        oct.to("timestamp", later);
      }
    });
    TestHelpers.assertThrows("toBefore(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        oct.toBefore("timestamp", later);
      }
    });

    // this behavior is debatable, but something odd is happening
    TestHelpers.assertThrows("from(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        oct.from("timestamp", later);
      }
    });
    TestHelpers.assertThrows("fromAfter(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        oct.fromAfter("timestamp", later);
      }
    });
  }

  @Test
  public void testUnboundedView() {
    // test events
    Assert.assertTrue("Should contain any StandardEvent",
        unbounded.canContain(event));
    Assert.assertTrue("Should contain even null events",
        unbounded.canContain((StandardEvent) null));
    Assert.assertTrue("Should contain older event",
        unbounded.canContain(sepEvent));
    Assert.assertTrue("Should contain event",
        unbounded.canContain(octEvent));
    Assert.assertTrue("Should contain newer event",
        unbounded.canContain(novEvent));

    // test range limiting
    Assert.assertNotNull("from should succeed",
        unbounded.from("timestamp", now));
    Assert.assertNotNull("fromAfter should succeed",
        unbounded.fromAfter("timestamp", now));
    Assert.assertNotNull("to should succeed",
        unbounded.to("timestamp", now));
    Assert.assertNotNull("toBefore should succeed",
        unbounded.toBefore("timestamp", now));
    Assert.assertNotNull("with should succeed",
        unbounded.with("timestamp", now));
  }

  @Test
  public void testNotPartitioned() throws Exception {
    final DatasetDescriptor flatDescriptor = new DatasetDescriptor
        .Builder(testDescriptor).partitionStrategy(null).build();
    final Dataset<StandardEvent> notPartitioned =
        repo.create("flat", flatDescriptor);

    // test events
    Assert.assertTrue("Should contain any StandardEvent",
        notPartitioned.canContain(event));
    Assert.assertTrue("Should contain even null events",
        notPartitioned.canContain((StandardEvent) null));
    Assert.assertTrue("Should contain older event",
        notPartitioned.canContain(sepEvent));
    Assert.assertTrue("Should contain event",
        notPartitioned.canContain(octEvent));
    Assert.assertTrue("Should contain newer event",
        notPartitioned.canContain(novEvent));

    // test range limiting
    Assert.assertNotNull("from should succeed",
        notPartitioned.from("timestamp", now));
    Assert.assertNotNull("fromAfter should succeed",
        notPartitioned.fromAfter("timestamp", now));
    Assert.assertNotNull("to should succeed",
        notPartitioned.to("timestamp", now));
    Assert.assertNotNull("toBefore should succeed",
        notPartitioned.toBefore("timestamp", now));
    Assert.assertNotNull("with should succeed",
        notPartitioned.with("timestamp", now));
  }
}
