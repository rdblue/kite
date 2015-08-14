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
package org.kitesdk.data.kudu;

import java.io.IOException;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Key;
import org.kitesdk.data.RandomAccessDataset;
import org.kitesdk.data.View;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.data.hbase.avro.entities.TestEntity;
import org.kitesdk.data.spi.AbstractRefinableView;

public class KuduDatasetRepositoryTest {
  private static final String[] NAMES = new String[] { "part1", "part2" };
  private static final String testEntity;
  private static final String tableName = "testtable";

  static {
    try {
      testEntity = AvroUtils
          .inputStreamToString(KuduDatasetRepositoryTest.class
              .getResourceAsStream("/TestEntity.avsc"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  @Test
  public void testSimple() {
    KuduDatasetRepository repo = new KuduDatasetRepository.Builder()
        .master("a1216.halxg.cloudera.com").build();
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral(testEntity).build();
    repo.delete("default", "something");
    RandomAccessDataset<TestEntity> dataset = repo
        .create("default", "something", descriptor, TestEntity.class);
    boolean response = dataset.put(
        TestEntity.newBuilder().setField1("field1").setField2("field2")
            .setPart1("part1").setPart2("part2").build());
    System.out.println(response);
    Key key = new Key.Builder(dataset).add("part1", "part1")
        .add("part2", "part2").build();
    DatasetReader<TestEntity> reader = dataset.newReader();
    for (TestEntity record : reader) {
      System.out.println(record);
    }
  }
}
