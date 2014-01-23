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
public abstract class TestRangeViews extends MiniDFSTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRangeViews.class);

  protected static final String[] Y = new String[] { "year" };
  protected static final String[] YM = new String[] { "year", "month" };
  protected static final String[] YMD = new String[] { "year", "month", "day" };
  protected static final String[] TIMESTAMP = new String[] { "timestamp" };
  protected static final String[] EMPTY = new String[0];
  protected static final long now = System.currentTimeMillis();
  protected static final StandardEvent event = StandardEvent.newBuilder()
      .setEventInitiator("TestRangeViews")
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

  protected TestRangeViews(boolean distributed) {
    this.distributed = distributed;
  }

  // from subclasses
  protected DatasetRepository repo = null;

  public abstract DatasetRepository newRepo();

  protected Configuration conf = null;
  protected FileSystem fs;
  protected PartitionStrategy strategy = null;
  protected DatasetDescriptor testDescriptor = null;
  protected Dataset<StandardEvent> testDataset = null;
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
    this.testDataset = repo.create("test", testDescriptor);
    this.unbounded = (RefineableView<StandardEvent>) testDataset;
  }

  @Test public abstract void testCoveringPartitions();

  @Test
  public void testRange() {
    final RefineableView<StandardEvent> range = unbounded
        .from(YM, 2013, 10).toBefore(YMD, 2013, 11, 14);

    Assert.assertTrue("Should contain Oct", range.contains(YM, 2013, 10));
    Assert.assertTrue("Should contain day in Oct",
        range.contains(YMD, 2013, 10, 5));
    Assert.assertFalse("Should not contain year",
        range.contains(Y, 2013));
    Assert.assertFalse("Should not contain previous month",
        range.contains(YM, 2013, 9));
    Assert.assertFalse("Should not contain next month",
        range.contains(YM, 2013, 11));
    Assert.assertFalse("Should not contain previous day",
        range.contains(YMD, 2013, 9, 30));
    Assert.assertTrue("Should contain Nov 11",
        range.contains(YMD, 2013, 11, 11));
    Assert.assertFalse("Should not contain Nov 14",
        range.contains(YMD, 2013, 11, 14));

    // test events
    Assert.assertFalse("Should not contain older event",
        range.contains(sepEvent));
    Assert.assertTrue("Should not contain event",
        range.contains(octEvent));
    Assert.assertTrue("Should contain newer event",
        range.contains(novEvent));
  }

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
    DatasetWriter<StandardEvent> writer = testDataset.newWriter();
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
        testDataset);

    // single bound
    assertContentEquals(Sets.newHashSet(octEvent, novEvent),
        unbounded.from(YMD, 2013, 10, 12));
    assertContentEquals(Sets.newHashSet(novEvent),
        unbounded.fromAfter(YMD, 2013, 10, 12));
    assertContentEquals(Sets.newHashSet(sepEvent, octEvent),
        unbounded.to(YMD, 2013, 10, 12));
    assertContentEquals(Sets.newHashSet(sepEvent),
        unbounded.toBefore(YMD, 2013, 10, 12));

    // in
    assertContentEquals(Sets.<StandardEvent>newHashSet(),
        unbounded.of(Y, 2012));
    assertContentEquals(Sets.newHashSet(sepEvent, octEvent, novEvent),
        unbounded.of(Y, 2013));
    assertContentEquals(Sets.<StandardEvent>newHashSet(),
        unbounded.of(YM, 2012, 10));
    assertContentEquals(Sets.newHashSet(octEvent),
        unbounded.of(YM, 2013, 10));
    assertContentEquals(Sets.newHashSet(octEvent),
        unbounded.of(YMD, 2013, 10, 12));
    assertContentEquals(Sets.<StandardEvent>newHashSet(),
        unbounded.of(YMD, 2013, 10, 11));

    // double bound
    assertContentEquals(Sets.newHashSet(sepEvent, octEvent, novEvent),
        unbounded.from(YM, 2013, 9).to(YM, 2013, 11));
    assertContentEquals(Sets.newHashSet(octEvent, novEvent),
        unbounded.from(YMD, 2013, 9, 22).to(YM, 2013, 11));
    assertContentEquals(Sets.newHashSet(octEvent, novEvent),
        unbounded.fromAfter(YMD, 2013, 9, 22).to(YM, 2013, 11));
    assertContentEquals(Sets.newHashSet(octEvent),
        unbounded.from(YMD, 2013, 9, 22).toBefore(YM, 2013, 11));
    assertContentEquals(Sets.newHashSet(sepEvent, octEvent),
        unbounded.from(YM, 2013, 9).to(YMD, 2013, 11, 4));
    assertContentEquals(Sets.newHashSet(octEvent),
        unbounded.fromAfter(YM, 2013, 9).to(YMD, 2013, 11, 4));
    assertContentEquals(Sets.newHashSet(sepEvent, octEvent),
        unbounded.from(YM, 2013, 9).toBefore(YMD, 2013, 11, 4));
    assertContentEquals(Sets.newHashSet(sepEvent, octEvent, novEvent),
        unbounded.from(YMD, 2013, 9, 1).to(YMD, 2013, 11, 31));
    assertContentEquals(Sets.newHashSet(octEvent, novEvent),
        unbounded.fromAfter(YMD, 2013, 9, 12).to(YMD, 2013, 11, 31));
    assertContentEquals(Sets.newHashSet(sepEvent, octEvent),
        unbounded.from(YMD, 2013, 9, 12).toBefore(YMD, 2013, 11, 11));
    assertContentEquals(Sets.<StandardEvent>newHashSet(),
        unbounded.from(Y, 2012).to(YM, 2013, 8));
  }

  @Test
  public void testLimitedWriter() {
    final RefineableView<StandardEvent> range = unbounded
        .from(YM, 2013, 10).toBefore(YMD, 2013, 11, 14);

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
    final RefineableView<StandardEvent> fromOctober = unbounded.from(YM, 2013, 10);

    Assert.assertTrue("Should contain partial", fromOctober.contains(YM, 2013, 10));
    Assert.assertTrue("Should contain days",
        fromOctober.contains(YMD, 2013, 10, 5));
    Assert.assertFalse("Should not contain year",
        fromOctober.contains(Y, 2013));
    Assert.assertFalse("Should not contain previous month",
        fromOctober.contains(YM, 2013, 9));
    Assert.assertTrue("Should contain next month",
        fromOctober.contains(YM, 2013, 11));
    Assert.assertFalse("Should not contain previous day",
        fromOctober.contains(YMD, 2013, 9, 30));
    Assert.assertTrue("Should contain next day",
        fromOctober.contains(YMD, 2013, 11, 1));

    // test events
    Assert.assertFalse("Should not contain older event",
        fromOctober.contains(sepEvent));
    Assert.assertTrue("Should contain event",
        fromOctober.contains(octEvent));
    Assert.assertTrue("Should contain newer event",
        fromOctober.contains(novEvent));
    Assert.assertTrue("Should contain current event",
        fromOctober.contains(event));

    // test limiting to a sub-view with contained markers
    Assert.assertNotNull("in(contained marker) should succeed",
        fromOctober.of(YMD, 2013, 10, 5));
    Assert.assertNotNull("from(contained marker) should succeed",
        fromOctober.of(YMD, 2013, 10, 6));
    Assert.assertNotNull("fromAfter(contained marker) should succeed",
        fromOctober.of(YMD, 2013, 10, 4));
    Assert.assertNotNull("to(contained marker) should succeed",
        fromOctober.to(YMD, 2013, 10, 21));
    Assert.assertNotNull("toBefore(contained marker) should succeed",
        fromOctober.toBefore(YMD, 2013, 10, 22));

    // test limiting to a sub-view with before markers
    TestHelpers.assertThrows("in(before marker) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        fromOctober.of(YMD, 2013, 9, 5);
      }
    });
    Assert.assertNotNull("from(before marker) should succeed",
        fromOctober.from(YMD, 2013, 9, 6));
    Assert.assertNotNull("fromAfter(before marker) should succeed",
        fromOctober.fromAfter(YMD, 2013, 9, 18));
    TestHelpers.assertThrows("to(before marker) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        fromOctober.to(YMD, 2013, 9, 21);
      }
    });
    TestHelpers.assertThrows("toBefore(before marker) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        fromOctober.toBefore(YMD, 2013, 9, 22);
      }
    });

    // test limiting to a sub-view with after markers
    Assert.assertNotNull("in(after marker) should succeed",
        fromOctober.of(YMD, 2013, 11, 19));
    Assert.assertNotNull("from(after marker) should succeed",
        fromOctober.from(YMD, 2013, 11, 1));
    Assert.assertNotNull("fromAfter(after marker) should succeed",
        fromOctober.fromAfter(YMD, 2013, 11, 1));
    Assert.assertNotNull("to(after marker) should succeed",
        fromOctober.to(YM, 2013, 11));
    Assert.assertNotNull("toBefore(after marker) should succeed",
        fromOctober.toBefore(YMD, 2013, 11, 4));

    // test limiting to a sub-view with a containing
    Assert.assertNotNull("in(not contained) should succeed",
        fromOctober.of(Y, 2013));
  }

  @Test
  public void testFromAfterView() {
    final RefineableView<StandardEvent> afterOct = unbounded.fromAfter(YM, 2013, 10);

    Assert.assertFalse("Should not contain partial", afterOct.contains(YM, 2013, 10));
    Assert.assertFalse("Should contain days",
        afterOct.contains(YMD, 2013, 10, 5));
    Assert.assertFalse("Should not contain year",
        afterOct.contains(Y, 2013));
    Assert.assertFalse("Should not contain previous month",
        afterOct.contains(YM, 2013, 9));
    Assert.assertTrue("Should contain next month",
        afterOct.contains(YM, 2013, 11));
    Assert.assertFalse("Should not contain previous day",
        afterOct.contains(YMD, 2013, 9, 30));
    Assert.assertTrue("Should contain next day",
        afterOct.contains(YMD, 2013, 11, 1));

    // test events
    Assert.assertFalse("Should not contain older event",
        afterOct.contains(sepEvent));
    Assert.assertFalse("Should not contain event",
        afterOct.contains(octEvent));
    Assert.assertTrue("Should contain newer event",
        afterOct.contains(novEvent));
    Assert.assertTrue("Should contain current event",
        afterOct.contains(event));

    // test limiting to a sub-view with contained markers
    TestHelpers.assertThrows("in(contained marker) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        afterOct.of(YMD, 2013, 10, 5);
      }
    });
    Assert.assertNotNull("from(contained marker) should succeed",
        afterOct.from(YMD, 2013, 10, 6));
    Assert.assertNotNull("fromAfter(contained marker) should succeed",
        afterOct.fromAfter(YMD, 2013, 10, 4));
    TestHelpers.assertThrows("to(contained marker) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        afterOct.to(YMD, 2013, 10, 21);
      }
    });
    TestHelpers.assertThrows("toBefore(contained marker) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        afterOct.toBefore(YMD, 2013, 10, 22);
      }
    });

    // test limiting to a sub-view with before markers
    TestHelpers.assertThrows("in(before marker) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        afterOct.of(YMD, 2013, 9, 5);
      }
    });
    Assert.assertNotNull("from(before marker) should succeed",
        afterOct.from(YMD, 2013, 9, 6));
    Assert.assertNotNull("fromAfter(before marker) should succeed",
        afterOct.fromAfter(YMD, 2013, 9, 18));
    TestHelpers.assertThrows("to(before marker) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        afterOct.to(YMD, 2013, 9, 21);
      }
    });
    TestHelpers.assertThrows("toBefore(before marker) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        afterOct.toBefore(YMD, 2013, 9, 22);
      }
    });

    // test limiting to a sub-view with after markers
    Assert.assertNotNull("in(after marker) should succeed",
        afterOct.of(YMD, 2013, 11, 19));
    Assert.assertNotNull("from(after marker) should succeed",
        afterOct.from(YMD, 2013, 11, 1));
    Assert.assertNotNull("fromAfter(after marker) should succeed",
        afterOct.fromAfter(YMD, 2013, 11, 1));
    Assert.assertNotNull("to(after marker) should succeed",
        afterOct.to(YM, 2013, 11));
    Assert.assertNotNull("toBefore(after marker) should succeed",
        afterOct.toBefore(YMD, 2013, 11, 4));

    // test limiting to a sub-view with a containing
    Assert.assertNotNull("in(not contained) should succeed",
        afterOct.of(Y, 2013));
  }

  @Test
  public void testToView() {
    final RefineableView<StandardEvent> toOctober = unbounded.to(YM, 2013, 10);

    Assert.assertTrue("Should contain partial", toOctober.contains(YM, 2013, 10));
    Assert.assertTrue("Should contain days",
        toOctober.contains(YMD, 2013, 10, 5));
    Assert.assertFalse("Should not contain year",
        toOctober.contains(Y, 2013));
    Assert.assertTrue("Should contain previous month",
        toOctober.contains(YM, 2013, 9));
    Assert.assertFalse("Should not contain next month",
        toOctober.contains(YM, 2013, 11));
    Assert.assertTrue("Should contain preceding day",
        toOctober.contains(YMD, 2013, 9, 30));
    Assert.assertFalse("Should not contain next day",
        toOctober.contains(YMD, 2013, 11, 1));

    // test events
    Assert.assertTrue("Should contain older event",
        toOctober.contains(sepEvent));
    Assert.assertTrue("Should contain event",
        toOctober.contains(octEvent));
    Assert.assertFalse("Should not contain newer event",
        toOctober.contains(novEvent));
    Assert.assertFalse("Should not contain current event",
        toOctober.contains(event));

    // test limiting to a sub-view with contained markers
    Assert.assertNotNull("in(contained marker) should succeed",
        toOctober.of(YMD, 2013, 10, 5));
    Assert.assertNotNull("from(contained marker) should succeed",
        toOctober.from(YMD, 2013, 10, 6));
    Assert.assertNotNull("fromAfter(contained marker) should succeed",
        toOctober.fromAfter(YMD, 2013, 10, 4));
    Assert.assertNotNull("to(contained marker) should succeed",
        toOctober.to(YMD, 2013, 10, 21));
    Assert.assertNotNull("toBefore(contained marker) should succeed",
        toOctober.toBefore(YMD, 2013, 10, 22));

    // test limiting to a sub-view with before markers
    Assert.assertNotNull("in(before marker) should succeed",
        toOctober.of(YMD, 2013, 9, 5));
    Assert.assertNotNull("from(before marker) should succeed",
        toOctober.from(YMD, 2013, 9, 6));
    Assert.assertNotNull("fromAfter(before marker) should succeed",
        toOctober.fromAfter(YMD, 2013, 9, 18));
    Assert.assertNotNull("to(before marker) should succeed",
        toOctober.to(YMD, 2013, 9, 21));
    Assert.assertNotNull("toBefore(before marker) should succeed",
        toOctober.toBefore(YMD, 2013, 9, 22));

    // test limiting to a sub-view with after markers
    TestHelpers.assertThrows("in(after marker) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        toOctober.of(YMD, 2013, 11, 19);
      }
    });
    TestHelpers.assertThrows("from(after marker) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        toOctober.fromAfter(YMD, 2013, 11, 1);
      }
    });
    TestHelpers.assertThrows("fromAfter(after marker) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        toOctober.fromAfter(YMD, 2013, 11, 1);
      }
    });
    Assert.assertNotNull("to(after marker) should succeed",
        toOctober.to(YM, 2013, 11));
    Assert.assertNotNull("toBefore(after marker) should succeed",
        toOctober.toBefore(YMD, 2013, 11, 4));

    // test limiting to a sub-view with a containing
    Assert.assertNotNull("in(not contained) should succeed",
        toOctober.of(Y, 2013));
  }

  @Test
  public void testToBeforeView() {
    final RefineableView<StandardEvent> beforeOct = unbounded.toBefore(YM, 2013, 10);

    Assert.assertFalse("Should not contain partial", beforeOct.contains(YM, 2013, 10));
    Assert.assertFalse("Should not contain days",
        beforeOct.contains(YMD, 2013, 10, 5));
    Assert.assertFalse("Should not contain year",
        beforeOct.contains(Y, 2013));
    Assert.assertTrue("Should contain previous month",
        beforeOct.contains(YM, 2013, 9));
    Assert.assertFalse("Should not contain next month",
        beforeOct.contains(YM, 2013, 11));
    Assert.assertTrue("Should contain previous day",
        beforeOct.contains(YMD, 2013, 9, 30));
    Assert.assertFalse("Should not contain next day",
        beforeOct.contains(YMD, 2013, 11, 1));

    // test events
    Assert.assertTrue("Should contain older event",
        beforeOct.contains(sepEvent));
    Assert.assertFalse("Should not contain event",
        beforeOct.contains(octEvent));
    Assert.assertFalse("Should not contain newer event",
        beforeOct.contains(novEvent));
    Assert.assertFalse("Should not contain current event",
        beforeOct.contains(event));

    // test limiting to a sub-view with contained markers
    TestHelpers.assertThrows("in(contained marker) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        beforeOct.of(YMD, 2013, 10, 5);
      }
    });
    TestHelpers.assertThrows("from(contained marker) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        beforeOct.fromAfter(YMD, 2013, 10, 6);
      }
    });
    TestHelpers.assertThrows("fromAfter(contained marker) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        beforeOct.fromAfter(YMD, 2013, 10, 4);
      }
    });
    Assert.assertNotNull("to(contained marker) should succeed",
        beforeOct.to(YMD, 2013, 10, 21));
    Assert.assertNotNull("toBefore(contained marker) should succeed",
        beforeOct.toBefore(YMD, 2013, 10, 22));

    // test limiting to a sub-view with before markers
    Assert.assertNotNull("in(before marker) should succeed",
        beforeOct.of(YMD, 2013, 9, 5));
    Assert.assertNotNull("from(before marker) should succeed",
        beforeOct.from(YMD, 2013, 9, 6));
    Assert.assertNotNull("fromAfter(before marker) should succeed",
        beforeOct.fromAfter(YMD, 2013, 9, 18));
    Assert.assertNotNull("to(before marker) should succeed",
        beforeOct.to(YM, 2013, 9));
    Assert.assertNotNull("toBefore(before marker) should succeed",
        beforeOct.toBefore(YMD, 2013, 9, 22));

    // test limiting to a sub-view with after markers
    TestHelpers.assertThrows("in(after marker) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        beforeOct.of(YMD, 2013, 11, 19);
      }
    });
    TestHelpers.assertThrows("from(after marker) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        beforeOct.fromAfter(YMD, 2013, 11, 1);
      }
    });
    TestHelpers.assertThrows("fromAfter(after marker) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        beforeOct.fromAfter(YMD, 2013, 11, 1);
      }
    });
    Assert.assertNotNull("to(after marker) should succeed",
        beforeOct.to(YM, 2013, 11));
    Assert.assertNotNull("toBefore(after marker) should succeed",
        beforeOct.toBefore(YMD, 2013, 11, 4));

    // test limiting to a sub-view with a containing
    Assert.assertNotNull("in(not contained) should succeed",
        beforeOct.of(Y, 2013));
  }

  @Test
  public void testInView() {
    final RefineableView<StandardEvent> inOctober = unbounded.of(YM, 2013, 10);

    Assert.assertTrue("Should contain partial", inOctober.contains(YM, 2013, 10));
    Assert.assertTrue("Should contain days",
        inOctober.contains(YMD, 2013, 10, 5));
    Assert.assertFalse("Should not contain year",
        inOctober.contains(Y, 2013));
    Assert.assertFalse("Should not contain previous month",
        inOctober.contains(YM, 2013, 9));
    Assert.assertFalse("Should not contain next month",
        inOctober.contains(YM, 2013, 11));
    Assert.assertFalse("Should not contain preceding day",
        inOctober.contains(YMD, 2013, 9, 30));
    Assert.assertFalse("Should not contain next day",
        inOctober.contains(YMD, 2013, 11, 1));

    // test events
    Assert.assertFalse("Should not contain older event",
        inOctober.contains(sepEvent));
    Assert.assertTrue("Should contain event",
        inOctober.contains(octEvent));
    Assert.assertFalse("Should not contain newer event",
        inOctober.contains(novEvent));
    Assert.assertFalse("Should not contain current event",
        inOctober.contains(event));

    // test limiting to a sub-view
    Assert.assertNotNull("in(contained marker) should succeed",
        inOctober.of(YMD, 2013, 10, 5));
    Assert.assertNotNull("from(contained marker) should succeed",
        inOctober.from(YMD, 2013, 10, 6));
    Assert.assertNotNull("fromAfter(contained marker) should succeed",
        inOctober.fromAfter(YMD, 2013, 10, 4));
    Assert.assertNotNull("to(contained marker) should succeed",
        inOctober.to(YMD, 2013, 10, 21));
    Assert.assertNotNull("toBefore(contained marker) should succeed",
        inOctober.toBefore(YMD, 2013, 10, 22));

    // test limiting to a sub-view with before markers
    TestHelpers.assertThrows("in(before marker) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        inOctober.of(YMD, 2013, 9, 5);
      }
    });
    Assert.assertNotNull("from(before marker) should succeed",
        inOctober.from(YMD, 2013, 9, 6));
    Assert.assertNotNull("fromAfter(before marker) should succeed",
        inOctober.fromAfter(YMD, 2013, 9, 18));
    TestHelpers.assertThrows("to(before marker) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        inOctober.to(YMD, 2013, 9, 21);
      }
    });
    TestHelpers.assertThrows("toBefore(before marker) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        inOctober.toBefore(YMD, 2013, 9, 22);
      }
    });

    // test limiting to a sub-view with after markers
    TestHelpers.assertThrows("in(after marker) should succeed",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        inOctober.of(YMD, 2013, 11, 19);
      }
    });
    TestHelpers.assertThrows("from(after marker) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        inOctober.fromAfter(YMD, 2013, 11, 1);
      }
    });
    TestHelpers.assertThrows("fromAfter(after marker) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        inOctober.fromAfter(YMD, 2013, 11, 1);
      }
    });
    Assert.assertNotNull("to(after marker) should succeed",
        inOctober.to(YM, 2013, 11));
    Assert.assertNotNull("toBefore(after marker) should succeed",
        inOctober.toBefore(YMD, 2013, 11, 4));

    // test limiting to a sub-view with a containing
    Assert.assertNotNull("in(not contained) should succeed",
        inOctober.of(Y, 2013));
  }

  @Test
  public void testUnboundedView() {
    Assert.assertTrue("Should contain any values",
        unbounded.contains(TIMESTAMP, now));
    Assert.assertTrue("Should contain an empty values",
        unbounded.contains(EMPTY));

    // test events
    Assert.assertTrue("Should contain any StandardEvent",
        unbounded.contains(event));
    Assert.assertTrue("Should contain even null events",
        unbounded.contains((StandardEvent) null));
    Assert.assertTrue("Should contain older event",
        unbounded.contains(sepEvent));
    Assert.assertTrue("Should contain event",
        unbounded.contains(octEvent));
    Assert.assertTrue("Should contain newer event",
        unbounded.contains(novEvent));

    // test range limiting
    Assert.assertNotNull("from should succeed",
        unbounded.from(Y, 2013));
    Assert.assertNotNull("fromAfter should succeed",
        unbounded.fromAfter(Y, 2013));
    Assert.assertNotNull("to should succeed",
        unbounded.to(Y, 2013));
    Assert.assertNotNull("toBefore should succeed",
        unbounded.toBefore(Y, 2013));
    Assert.assertNotNull("in should succeed",
        unbounded.of(Y, 2013));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testNotPartitioned() throws Exception {
    final DatasetDescriptor flatDescriptor = new DatasetDescriptor
        .Builder(testDescriptor).partitionStrategy(null).build();
    final Dataset<StandardEvent> flatDataset =
        repo.create("flat", flatDescriptor);
    final RefineableView<StandardEvent> notPartitioned = (RefineableView<StandardEvent>)
        flatDataset;

    // test contains(names, values)
    Assert.assertTrue("Should contain any values",
        notPartitioned.contains(TIMESTAMP, now));
    Assert.assertTrue("Should contain an empty values",
        notPartitioned.contains(EMPTY));

    // test contains(Entity)
    Assert.assertTrue("Should contain any StandardEvent",
        notPartitioned.contains(event));
    Assert.assertTrue("Should contain even null events",
        notPartitioned.contains((StandardEvent) null));

    TestHelpers.assertThrows("from should fail",
        IllegalStateException.class, new Runnable() {
      @Override
      public void run() {
        notPartitioned.from(TIMESTAMP, now);
      }
    });
    TestHelpers.assertThrows("fromAfter should fail",
        IllegalStateException.class, new Runnable() {
      @Override
      public void run() {
        notPartitioned.fromAfter(TIMESTAMP, now);
      }
    });
    TestHelpers.assertThrows("to should fail",
        IllegalStateException.class, new Runnable() {
      @Override
      public void run() {
        notPartitioned.to(TIMESTAMP, now);
      }
    });
    TestHelpers.assertThrows("toBefore should fail",
        IllegalStateException.class, new Runnable() {
      @Override
      public void run() {
        notPartitioned.toBefore(TIMESTAMP, now);
      }
    });
    TestHelpers.assertThrows("in should fail",
        IllegalStateException.class, new Runnable() {
      @Override
      public void run() {
        notPartitioned.of(TIMESTAMP, now);
      }
    });
  }

}
