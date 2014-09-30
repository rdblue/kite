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

package org.kitesdk.data.spi.filesystem;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.Formats;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.TestHelpers;

import static org.apache.avro.generic.GenericData.Record;

/**
 * This tests the FileSystemWriter when it is backed by DurableParquetAppender.
 *
 * This must be a MiniDFSTest because LocalFileSystem doesn't support hflush
 * and hsync.
 */
public class TestDurableParquetWriter extends MiniDFSTest {

  private static final Schema SCHEMA = SchemaBuilder.record("Point").fields()
      .requiredDouble("x")
      .requiredDouble("y")
      .endRecord();

  private static final GenericRecordBuilder RECORD_BUILDER =
      new GenericRecordBuilder(SCHEMA);

  private static final List<Record> RECORDS = Lists.newArrayList(
      RECORD_BUILDER.set("x", 0.0).set("y", 0.0).build(),
      RECORD_BUILDER.set("x", 1.0).set("y", 0.0).build(),
      RECORD_BUILDER.set("x", 0.0).set("y", 1.0).build());

  private static final Record UNCOMMITTED = RECORD_BUILDER
      .set("x", 1.0).set("y", 1.0).build();

  private static final DatasetDescriptor DESCRIPTOR = new DatasetDescriptor
      .Builder()
      .schema(SCHEMA)
      .format(Formats.PARQUET)
      .build();

  @Test
  public void testEntityWriteFailureAppendWriteException() throws IOException {
    final FileSystemWriter<Record> writer = new FileSystemWriter<Record>(
        getDFS(), new Path("/tmp"), DESCRIPTOR);

    try {
      writer.initialize();

      final Record badRecord = RECORD_BUILDER.set("x", 1.0).set("y", 1.0).build();
      badRecord.put(1, "cheese!");

      for (Record rec : RECORDS) {
        writer.write(rec);
      }

      TestHelpers.assertThrows("Bad record should cause an exception",
          DataFileWriter.AppendWriteException.class, new Runnable() {
            @Override
            public void run() {
              writer.write(badRecord);
            }
          });

      Assert.assertTrue("Should be open", writer.isOpen());

      writer.close();

      Assert.assertTrue("Should have committed a file",
          getDFS().exists(writer.finalPath));
      Assert.assertEquals("Successfully written records should be present",
          Sets.newHashSet(RECORDS), readParquet(writer.finalPath, Record.class));

    } finally {
      cleanUp(writer);
    }
  }

  @Test
  public void testEntityWriteRuntimeException() throws IOException {
    final FileSystemWriter<Record> writer = new FileSystemWriter<Record>(
        getDFS(), new Path("/tmp"), DESCRIPTOR);

    try {
      writer.initialize();

      // get rid of the existing appender
      writer.appender.close();
      getDFS().delete(writer.tempPath, false /* not recursive, not a dir */ );

      // replace the appender with one that will fail on the right record
      writer.appender = new DurableParquetAppender<Record>(
          getDFS(), writer.tempPath, SCHEMA, getConfiguration(),
          Formats.PARQUET.getDefaultCompressionType()) {
        private int count = 0;
        private int failOn = RECORDS.size() + 2;

        @Override
        public void append(Record entity) throws IOException {
          count += 1;
          if (failOn == count) {
            throw new RuntimeException("Not AppendWriteException");
          }
          super.append(entity);
        }
      };
      writer.appender.open();

      for (Record rec : RECORDS) {
        writer.write(rec);
      }

      writer.flush();
      writer.sync();

      writer.write(UNCOMMITTED);

      final Record failRecord = RECORD_BUILDER.set("x", 2.0).set("y", 1.0).build();

      TestHelpers.assertThrows("Bad record should cause an exception",
          RuntimeException.class, new Runnable() {
            @Override
            public void run() {
              writer.write(failRecord);
            }
          });

      Assert.assertFalse("Writer should be in an error state, not open",
          writer.isOpen());

      assertNotCommitted(writer);
      assertAvroRecoveryFileContents(writer, RECORDS);

      writer.close();

      assertNotCommitted(writer);
      assertAvroRecoveryFileContents(writer, RECORDS);

    } finally {
      cleanUp(writer);
    }
  }

  @Test
  public void testCloseIOException() throws IOException {
    final FileSystemWriter<Record> writer = new FileSystemWriter<Record>(
        getDFS(), new Path("/tmp"), DESCRIPTOR);

    try {
      writer.initialize();

      // get rid of the existing appender
      writer.appender.close();
      getDFS().delete(writer.tempPath, false /* not recursive, not a dir */ );

      // replace the appender with one that will fail in close
      writer.appender = new DurableParquetAppender<Record>(
          getDFS(), writer.tempPath, SCHEMA, getConfiguration(),
          Formats.PARQUET.getDefaultCompressionType()) {
        private boolean failed = false;

        @Override
        public void close() throws IOException {
          if (!failed) {
            failed = true;
            throw new IOException("Failed to do anything");
          }
          super.close();
        }
      };
      writer.appender.open();

      for (Record rec : RECORDS) {
        writer.write(rec);
      }

      // end the current batch by flushing/syncing
      writer.flush();
      writer.sync();

      writer.write(UNCOMMITTED);

      TestHelpers.assertThrows("Close should fail with an IOException",
          DatasetIOException.class, new Runnable() {
            @Override
            public void run() {
              writer.close();
            }
          });

      Assert.assertFalse("Writer should be in an error state, not open",
          writer.isOpen());

      assertNotCommitted(writer);
      assertAvroRecoveryFileContents(writer, RECORDS);

      writer.close();

      assertNotCommitted(writer);
      assertAvroRecoveryFileContents(writer, RECORDS);

    } finally {
      cleanUp(writer);
    }
  }

  @Test
  public void testCloseRuntimeException() throws IOException {
    final FileSystemWriter<Record> writer = new FileSystemWriter<Record>(
        getDFS(), new Path("/tmp"), DESCRIPTOR);

    try {
      writer.initialize();

      // get rid of the existing appender
      writer.appender.close();
      getDFS().delete(writer.tempPath, false /* not recursive, not a dir */ );

      // replace the appender with one that will fail in close
      writer.appender = new DurableParquetAppender<Record>(
          getDFS(), writer.tempPath, SCHEMA, getConfiguration(),
          Formats.PARQUET.getDefaultCompressionType()) {
        private boolean failed = false;

        @Override
        public void close() throws IOException {
          if (!failed) {
            failed = true;
            throw new RuntimeException("Failed to do anything");
          }
          super.close();
        }
      };
      writer.appender.open();

      for (Record rec : RECORDS) {
        writer.write(rec);
      }

      // end the current batch by flushing/syncing
      writer.flush();
      writer.sync();

      writer.write(UNCOMMITTED);

      TestHelpers.assertThrows("Close should fail with a RuntimeException",
          RuntimeException.class, new Runnable() {
            @Override
            public void run() {
              writer.close();
            }
          });

      Assert.assertFalse("Writer should be in an error state, not open",
          writer.isOpen());

      assertNotCommitted(writer);
      assertAvroRecoveryFileContents(writer, RECORDS);

      writer.close();

      assertNotCommitted(writer);
      assertAvroRecoveryFileContents(writer, RECORDS);

    } finally {
      cleanUp(writer);
    }
  }

  @Test
  public void testFlushRuntimeException() throws IOException {
    final FileSystemWriter<Record> writer = new FileSystemWriter<Record>(
        getDFS(), new Path("/tmp"), DESCRIPTOR);

    try {
      writer.initialize();

      // get rid of the existing appender
      writer.appender.close();
      getDFS().delete(writer.tempPath, false /* not recursive, not a dir */ );

      // replace the appender with one that will fail in flush
      writer.appender = new DurableParquetAppender<Record>(
          getDFS(), writer.tempPath, SCHEMA, getConfiguration(),
          Formats.PARQUET.getDefaultCompressionType()) {
        private int count = 0;
        private final int failOn = 2; // second call to flush fails

        @Override
        public void flush() throws IOException {
          this.count += 1;
          if (failOn == count) {
            throw new RuntimeException("Failed to do anything");
          }
          super.flush();
        }
      };
      writer.appender.open();

      for (Record rec : RECORDS) {
        writer.write(rec);
      }

      writer.flush();
      writer.sync();

      writer.write(UNCOMMITTED);

      TestHelpers.assertThrows("Flush should fail with a RuntimeException",
          RuntimeException.class, new Runnable() {
            @Override
            public void run() {
              writer.flush();
            }
          });

      Assert.assertFalse("Writer should be in an error state, not open",
          writer.isOpen());

      assertNotCommitted(writer);
      assertAvroRecoveryFileContents(writer, RECORDS);

      writer.close();

      assertNotCommitted(writer);
      assertAvroRecoveryFileContents(writer, RECORDS);

    } finally {
      cleanUp(writer);
    }
  }

  @Test
  public void testFlushIOException() throws IOException {
    final FileSystemWriter<Record> writer = new FileSystemWriter<Record>(
        getDFS(), new Path("/tmp"), DESCRIPTOR);

    try {
      writer.initialize();

      // get rid of the existing appender
      writer.appender.close();
      getDFS().delete(writer.tempPath, false /* not recursive, not a dir */ );

      // replace the appender with one that will fail in flush
      writer.appender = new DurableParquetAppender<Record>(
          getDFS(), writer.tempPath, SCHEMA, getConfiguration(),
          Formats.PARQUET.getDefaultCompressionType()) {
        private int count = 0;
        private final int failOn = 2; // second call to flush fails

        @Override
        public void flush() throws IOException {
          this.count += 1;
          if (failOn == count) {
            throw new IOException("Failed to do anything");
          }
          super.flush();
        }
      };
      writer.appender.open();

      for (Record rec : RECORDS) {
        writer.write(rec);
      }

      writer.flush();
      writer.sync();

      writer.write(UNCOMMITTED);

      TestHelpers.assertThrows("Flush should fail with an DatasetIOException",
          DatasetIOException.class, new Runnable() {
            @Override
            public void run() {
              writer.flush();
            }
          });

      Assert.assertFalse("Writer should be in an error state, not open",
          writer.isOpen());

      assertNotCommitted(writer);
      assertAvroRecoveryFileContents(writer, RECORDS);

      writer.close();

      assertNotCommitted(writer);
      assertAvroRecoveryFileContents(writer, RECORDS);

    } finally {
      cleanUp(writer);
    }
  }

  @Test
  public void testSyncRuntimeException() throws IOException {
    final FileSystemWriter<Record> writer = new FileSystemWriter<Record>(
        getDFS(), new Path("/tmp"), DESCRIPTOR);

    try {
      writer.initialize();

      // get rid of the existing appender
      writer.appender.close();
      getDFS().delete(writer.tempPath, false /* not recursive, not a dir */ );

      // replace the appender with one that will fail in sync
      writer.appender = new DurableParquetAppender<Record>(
          getDFS(), writer.tempPath, SCHEMA, getConfiguration(),
          Formats.PARQUET.getDefaultCompressionType()) {
        private int count = 0;
        private final int failOn = 2; // second call to sync fails

        @Override
        public void sync() throws IOException {
          this.count += 1;
          if (failOn == count) {
            throw new RuntimeException("Failed to do anything");
          }
          super.flush();
        }
      };
      writer.appender.open();

      for (Record rec : RECORDS) {
        writer.write(rec);
      }

      writer.flush();
      writer.sync();

      writer.write(UNCOMMITTED);

      writer.flush();

      TestHelpers.assertThrows("Sync should fail with a RuntimeException",
          RuntimeException.class, new Runnable() {
            @Override
            public void run() {
              writer.sync();
            }
          });

      Assert.assertFalse("Writer should be in an error state, not open",
          writer.isOpen());

      assertNotCommitted(writer);
      // use contains because the uncommitted record may or may not be present
      assertAvroRecoveryFileContains(writer, RECORDS);

      writer.close();

      assertNotCommitted(writer);
      // use contains because the uncommitted record may or may not be present
      assertAvroRecoveryFileContains(writer, RECORDS);

    } finally {
      cleanUp(writer);
    }
  }

  @Test
  public void testSyncIOException() throws IOException {
    final FileSystemWriter<Record> writer = new FileSystemWriter<Record>(
        getDFS(), new Path("/tmp"), DESCRIPTOR);

    try {
      writer.initialize();

      // get rid of the existing appender
      writer.appender.close();
      getDFS().delete(writer.tempPath, false /* not recursive, not a dir */ );

      // replace the appender with one that will fail in sync
      writer.appender = new DurableParquetAppender<Record>(
          getDFS(), writer.tempPath, SCHEMA, getConfiguration(),
          Formats.PARQUET.getDefaultCompressionType()) {
        private int count = 0;
        private final int failOn = 2; // second call to sync fails

        @Override
        public void sync() throws IOException {
          this.count += 1;
          if (failOn == count) {
            throw new IOException("Failed to do anything");
          }
          super.flush();
        }
      };
      writer.appender.open();

      for (Record rec : RECORDS) {
        writer.write(rec);
      }

      writer.flush();
      writer.sync();

      writer.write(UNCOMMITTED);

      writer.flush();

      TestHelpers.assertThrows("Sync should fail with an DatasetIOException",
          DatasetIOException.class, new Runnable() {
            @Override
            public void run() {
              writer.sync();
            }
          });

      Assert.assertFalse("Writer should be in an error state, not open",
          writer.isOpen());

      assertNotCommitted(writer);
      // use contains because the uncommitted record may or may not be present
      assertAvroRecoveryFileContains(writer, RECORDS);

      writer.close();

      assertNotCommitted(writer);
      // use contains because the uncommitted record may or may not be present
      assertAvroRecoveryFileContains(writer, RECORDS);

    } finally {
      cleanUp(writer);
    }
  }

  @Test
  public void testNeverClosed() throws IOException {
    final FileSystemWriter<Record> writer = new FileSystemWriter<Record>(
        getDFS(), new Path("/tmp"), DESCRIPTOR);

    try {
      writer.initialize();

      for (Record rec : RECORDS) {
        writer.write(rec);
      }

      // end the current batch by flushing/syncing
      writer.flush();
      writer.sync();

      writer.write(UNCOMMITTED);

      assertNotCommitted(writer);
      assertAvroRecoveryFileContents(writer, RECORDS);

    } finally {
      cleanUp(writer);
    }
  }

  private void cleanUp(FileSystemWriter<?> writer) throws IOException {
    if (writer.isOpen()) {
      writer.close();
    }
    FileSystem fs = getDFS();
    if (fs.exists(writer.finalPath)) {
      fs.delete(writer.finalPath, false /* not recursive, not a dir */);
    }
    if (fs.exists(writer.tempPath)) {
      fs.delete(writer.tempPath, false /* not recursive, not a dir */);
    }
    if (fs.exists(DurableParquetAppender.avroPath(writer.tempPath))) {
      fs.delete(DurableParquetAppender.avroPath(writer.tempPath),
          false /* not recursive, not a dir */);
    }
  }

  private <E extends IndexedRecord> Set<E> readParquet(Path path, Class<E> type) {
    ParquetFileSystemDatasetReader<E> reader = new ParquetFileSystemDatasetReader<E>(
        getDFS(), path, SCHEMA, type);
    try {
      reader.initialize();
      return Sets.newHashSet((Iterable<E>) reader);
    } finally {
      reader.close();
    }
  }

  private static <E> Set<E> readAvro(Path path, Class<E> type) {
    FileSystemDatasetReader<E> reader = new FileSystemDatasetReader<E>(
        getDFS(), path, SCHEMA, type);
    try {
      reader.initialize();
      return Sets.newHashSet((Iterable<E>) reader);
    } finally {
      reader.close();
    }
  }

  private static void assertNotCommitted(FileSystemWriter writer) throws IOException {
    Assert.assertFalse("Should not have committed, close was not called",
        getDFS().exists(writer.finalPath));
    Assert.assertTrue("Should have a parquet temp file",
        getDFS().exists(writer.tempPath));
  }

  private static <E> void assertAvroRecoveryFileContents(
      FileSystemWriter writer, Iterable<E> records) throws IOException {
    Path avroPath = DurableParquetAppender.avroPath(writer.tempPath);
    Assert.assertTrue("Should have an avro temp file",
        getDFS().exists(avroPath));

    Set<Record> avroContents = readAvro(avroPath, Record.class);
    Assert.assertEquals("Successfully written records should be present",
        Sets.newHashSet(records), avroContents);
  }

  private static <E> void assertAvroRecoveryFileContains(
      FileSystemWriter writer, Iterable<E> records) throws IOException {
    Path avroPath = DurableParquetAppender.avroPath(writer.tempPath);
    Assert.assertTrue("Should have an avro temp file",
        getDFS().exists(avroPath));

    Set<Record> avroContents = readAvro(avroPath, Record.class);
    for (E record : records) {
      Assert.assertTrue("Successfully written records should be present",
          avroContents.contains(record));
    }
  }
}
