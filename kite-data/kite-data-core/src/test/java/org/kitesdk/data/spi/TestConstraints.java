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

import java.util.Random;
import java.util.UUID;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.data.PartitionStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestConstraints {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestConstraints.class);

  public static final long START = System.currentTimeMillis();
  public static final Random RANDOM = new Random(1234);
  public static final long ONE_DAY_MILLIS = 86400000; // 24 * 60 * 60 * 1000

  public static class GenericEvent {
    public String id = UUID.randomUUID().toString();
    public long timestamp = START + RANDOM.nextInt(100000);

    public String getId() {
      return id;
    }

    public long getTimestamp() {
      return timestamp;
    }
  }


  private PartitionStrategy timeOnly = new PartitionStrategy.Builder()
      .year("timestamp")
      .month("timestamp")
      .day("timestamp")
      .build();
  private PartitionStrategy strategy = new PartitionStrategy.Builder()
      .hash("id", "id-hash", 64)
      .year("timestamp")
      .month("timestamp")
      .day("timestamp")
      .identity("id", String.class, 100000)
      .build();

  @Test
  public void testBasicMatches() {
    GenericEvent e = new GenericEvent();
    StorageKey key = new StorageKey(strategy).reuseFor(e);

    Constraints<GenericEvent> time = new Constraints<GenericEvent>()
        .from("timestamp", START)
        .to("timestamp", START + 100000);
    Assert.assertTrue(time.matchesKey(key));
    Assert.assertTrue(time.contains(e));

    Constraints<GenericEvent> timeAndUUID = time.with("id", e.getId());
    Assert.assertTrue(timeAndUUID.matchesKey(key));
    Assert.assertTrue(timeAndUUID.contains(e));

    // just outside the actual range should match partition but not event
    e.timestamp = START - 1;
    key.reuseFor(e);
    Assert.assertFalse(time.contains(e));
    Assert.assertFalse(timeAndUUID.contains(e));
    Assert.assertTrue(time.matchesKey(key));
    Assert.assertTrue(timeAndUUID.matchesKey(key));

    // just outside the actual range should match partition but not event
    e.timestamp = START - 100001;
    key.reuseFor(e);
    Assert.assertFalse(time.contains(e));
    Assert.assertFalse(timeAndUUID.contains(e));
    Assert.assertTrue(time.matchesKey(key));
    Assert.assertTrue(timeAndUUID.matchesKey(key));

    // a different day will cause the partition to stop matching
    e.timestamp = START - ONE_DAY_MILLIS;
    key.reuseFor(e);
    Assert.assertFalse(time.contains(e));
    Assert.assertFalse(timeAndUUID.contains(e));
    Assert.assertFalse(time.matchesKey(key));
    Assert.assertFalse(timeAndUUID.matchesKey(key));
  }

  @Test
  public void testTimeRangeEdgeCases() {
    final long oct_24_2013 = new DateTime(2013, 10, 24, 0, 0, DateTimeZone.UTC).getMillis();
    final long oct_25_2013 = new DateTime(2013, 10, 25, 0, 0, DateTimeZone.UTC).getMillis();
    final long oct_24_2013_end = new DateTime(2013, 10, 24, 23, 59, 59, 999, DateTimeZone.UTC).getMillis();
    //final long oct_24_2013_end = oct_25_2013 - 1;
    GenericEvent e = new GenericEvent();

    e.timestamp = oct_25_2013;
    StorageKey key = new StorageKey(timeOnly).reuseFor(e);

    Constraints c = new Constraints().with("timestamp", oct_24_2013);
    Assert.assertFalse("Should not match", c.matchesKey(key));

    c = new Constraints().toBefore("timestamp", oct_25_2013);
    c = new Constraints().toBefore("timestamp", oct_24_2013_end);
    LOG.info("Constraints: {}", c);

    e.timestamp = oct_25_2013;
    key.reuseFor(e);
    Assert.assertFalse("Should not match toBefore", c.matchesKey(key));
  }
}
