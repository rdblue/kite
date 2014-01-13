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

package org.kitesdk.data.filesystem;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.net.URI;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.View;
import org.kitesdk.data.event.StandardEvent;
import org.kitesdk.data.spi.Marker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSimpleView {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestSimpleView.class);

  protected static final Marker october = newMarker(2013, 10);
  protected static final Marker now = new Marker
      .Builder("timestamp", System.currentTimeMillis()).build();
  protected static final Marker empty = new Marker.Builder().build();
  protected static final StandardEvent event = StandardEvent.newBuilder()
      .setEventInitiator("TestRangeViews")
      .setEventName("TestEvent")
      .setUserId(0)
      .setSessionId("session-0")
      .setIp("localhost")
      .setTimestamp(System.currentTimeMillis() + 35405168l)
      .build();
  protected static final StandardEvent sepEvent = StandardEvent
      .newBuilder(event)
      .setTimestamp(1379020547042l) // Thu Sep 12 14:15:47 PDT 2013
      .build();
  protected static final StandardEvent octEvent = StandardEvent
      .newBuilder(event)
      .setTimestamp(1381612547042l) // Sat Oct 12 14:15:47 PDT 2013
      .setUserId(1)
      .build();
  protected static final StandardEvent novEvent = StandardEvent
      .newBuilder(event)
      .setTimestamp(1384204547042l) // Mon Nov 11 13:15:47 PST 2013
      .build();

  // from subclasses
  protected DatasetRepository repo = null;

  public DatasetRepository newRepo() {
    return new FileSystemDatasetRepository.Builder()
        .configuration(conf)
        .rootDirectory(URI.create("target/data"))
        .build();
  }

  protected Configuration conf = null;
  protected FileSystem fs;
  protected PartitionStrategy strategy = null;
  protected DatasetDescriptor testDescriptor = null;
  protected Dataset<StandardEvent> testDataset = null;

  @Before
  public void setup() throws Exception {
    this.conf = new Configuration();
    this.fs = FileSystem.get(conf);

    this.repo = newRepo();
    this.strategy = new PartitionStrategy.Builder()
        .month("timestamp")
        .hash("user_id", 2)
        .build();
    this.testDescriptor = new DatasetDescriptor.Builder()
        .schemaUri("resource:standard_event.avsc")
        .partitionStrategy(strategy)
        .build();
    this.testDataset = repo.create("test", testDescriptor);
  }

  @After
  public void removeDataPath() throws IOException {
    fs.delete(new Path("target/data"), true);
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
        testDataset.from("month", 10));
    assertContentEquals(Sets.newHashSet(novEvent),
        testDataset.fromAfter("month", 10));
    assertContentEquals(Sets.newHashSet(sepEvent, octEvent),
        testDataset.to("month", 10));
    assertContentEquals(Sets.newHashSet(sepEvent),
        testDataset.toBefore("month", 10));

    // double bound
    assertContentEquals(Sets.newHashSet(octEvent),
        testDataset.from("month", 10).toBefore("month", 11));

    // with
    assertContentEquals(Sets.newHashSet(sepEvent, octEvent, novEvent),
        testDataset.with("month"));
    assertContentEquals(Sets.newHashSet(octEvent),
        testDataset.with("month", 10));
    assertContentEquals(Sets.newHashSet(sepEvent, novEvent),
        testDataset.with("user_id", 0));
    assertContentEquals(Sets.newHashSet(sepEvent),
        testDataset.with("user_id", 0).with("month", 9));
    assertContentEquals(Sets.newHashSet(sepEvent),
        testDataset.with("month", 9).with("user_id", 0));

    // union
    assertContentEquals(Sets.newHashSet(sepEvent, novEvent),
        testDataset.from("month", 9).to("month", 9)
            .union(testDataset.from("month", 11).to("month", 11)));

    // complement
    assertContentEquals(Sets.newHashSet(sepEvent),
        testDataset.from("month", 10).complement());

  }

  public static Marker newMarker(Object... values) {
    Marker.Builder builder = new Marker.Builder();
    if (values.length >= 1) {
      builder.add("year", values[0]);
      if (values.length >= 2) {
        builder.add("month", values[1]);
        if (values.length >= 3) {
          builder.add("day", values[2]);
        }
      }
    }
    return builder.build();
  }

}
