/*
 * Copyright 2015 Cloudera Inc.
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

package org.kitesdk.data.spi.filesystem;

import com.google.common.io.Files;
import java.io.IOException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Formats;
import org.kitesdk.data.LocalFileSystem;
import org.kitesdk.data.TestDatasetReaders;
import org.kitesdk.data.event.ReflectStandardEvent;
import org.kitesdk.data.event.StandardEvent;
import org.kitesdk.data.spi.filesystem.DatasetTestUtilities.RecordValidator;

public class TestWriteSpecificReadReflectParquet extends TestDatasetReaders<ReflectStandardEvent> {

  private static final int totalRecords = 100;
  protected static FileSystem fs = null;
  protected static Path testDirectory = null;
  protected static Dataset<ReflectStandardEvent> readerDataset;

  @BeforeClass
  public static void setup() throws IOException {
    fs = LocalFileSystem.getInstance();
    testDirectory = new Path(Files.createTempDir().getAbsolutePath());
    FileSystemDatasetRepository repo = new FileSystemDatasetRepository(
        fs.getConf(), testDirectory);
    Dataset<StandardEvent> writerDataset = repo.create("ns", "test",
        new DatasetDescriptor.Builder()
            .schema(StandardEvent.class)
            .format(Formats.PARQUET)
            .build(),
        StandardEvent.class);
    DatasetWriter<StandardEvent> writer = writerDataset.newWriter();
    for (long i = 0; i < totalRecords; i++) {
      String text = String.valueOf(i);
      writer.write(new StandardEvent(text, text, i, text, text, i));
    }
    writer.close();

    readerDataset = repo.load("ns", "test", ReflectStandardEvent.class);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    fs.delete(testDirectory, true);
  }

  @Override
  public DatasetReader<ReflectStandardEvent> newReader() throws IOException {
    return readerDataset.newReader();
  }

  @Override
  public int getTotalRecords() {
    return totalRecords;
  }

  @Override
  public RecordValidator<ReflectStandardEvent> getValidator() {
    return new RecordValidator<ReflectStandardEvent>() {
      @Override
      public void validate(ReflectStandardEvent record, int recordNum) {
        Assert.assertEquals(String.valueOf(recordNum), record.getEvent_initiator());
        Assert.assertEquals(String.valueOf(recordNum), record.getEvent_name());
        Assert.assertEquals((long) recordNum, record.getUser_id());
        Assert.assertEquals(String.valueOf(recordNum), record.getSession_id());
        Assert.assertEquals(String.valueOf(recordNum), record.getIp());
        Assert.assertEquals((long) recordNum, record.getTimestamp());
      }
    };
  }
}
