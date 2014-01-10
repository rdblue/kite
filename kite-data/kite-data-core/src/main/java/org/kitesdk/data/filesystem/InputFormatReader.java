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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.DatasetReaderException;
import org.kitesdk.data.spi.AbstractDatasetReader;
import org.kitesdk.data.Pair;
import org.kitesdk.data.spi.ReaderWriterState;

public class InputFormatReader<K, V> extends AbstractDatasetReader<Pair<K, V>> {
  public static final String INPUT_FORMAT_CLASS_PROP = "kite.inputformat.class";

  private static final Constructor CONTEXT_CONSTRUCTOR;
  static {
    // setup for newJob and newTaskAttemptContext
    Class<?> taskContextClass;
    try {
      taskContextClass = Class.forName(
          "org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl");
    } catch (ClassNotFoundException mustBeHadoop1) {
      try {
        taskContextClass = Class.forName(
            "org.apache.hadoop.mapreduce.TaskAttemptContext");
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Couldn't find class", e);
      }
    }
    try {
      CONTEXT_CONSTRUCTOR = taskContextClass.getConstructor(
          Configuration.class, TaskAttemptID.class);
      CONTEXT_CONSTRUCTOR.setAccessible(true);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("Couldn't find constructor", e);
    }
  }

  private final Path path;
  private final Iterator<InputSplit> splits;
  private final FileInputFormat<K, V> format;
  private final Configuration conf;
  private final TaskAttemptContext attemptContext;

  // reader state
  private ReaderWriterState state = ReaderWriterState.NEW;
  private RecordReader<K, V> currentReader = null;
  private boolean hasNext = false;
  private boolean shouldAdvance = false;

  public InputFormatReader(FileSystem fs, Path path, DatasetDescriptor descriptor) {
    this.path = path;
    this.state = ReaderWriterState.NEW;
    this.format = newFormatInstance(descriptor);
    // set up the configuration from the descriptor properties
    this.conf = new Configuration(fs.getConf());
    for (String prop : descriptor.listProperties()) {
      conf.set(prop, descriptor.getProperty(prop));
    }
    this.attemptContext = newTaskAttemptContext(conf);
    try {
      Job job = new Job(conf);
      FileInputFormat.addInputPath(job, path);
      // attempt to minimize the number of InputSplits
      FileStatus stat = fs.getFileStatus(path);
      FileInputFormat.setMaxInputSplitSize(job, stat.getLen());
      this.splits = format.getSplits(job).iterator();
    } catch (IOException e) {
      throw new DatasetIOException("Cannot determine InputSplits", e);
    }
  }

  @Override
  public void open() {
    Preconditions.checkState(ReaderWriterState.NEW.equals(state),
        "A reader may not be opened more than once - current state:%s", state);

    this.shouldAdvance = true;
    this.state = ReaderWriterState.OPEN;
  }

  @Override
  public boolean hasNext() {
    Preconditions.checkState(ReaderWriterState.OPEN.equals(state),
        "Attempt to read from a file in state:%s", state);

    // the Iterator contract requires that calls to hasNext() not change the
    // iterator state. calling next() should advance the iterator. however,
    // this wraps a RecordReader that reuses objects, so advancing in next
    // after retrieving the key/value pair mutates the pair. this hack is a way
    // to advance once per call to next(), but do it as late as possible.
    if (shouldAdvance) {
      this.hasNext = advance();
      this.shouldAdvance = false;
    }
    return hasNext;
  }

  @Override
  public Pair<K, V> next() {
    Preconditions.checkState(ReaderWriterState.OPEN.equals(state),
        "Attempt to read from a file in state:%s", state);

    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    try {
      Pair<K, V> pair = Pair.of(
          currentReader.getCurrentKey(), currentReader.getCurrentValue());

      this.shouldAdvance = true;

      return pair;
    } catch (IOException e) {
      throw new DatasetIOException("Cannot get key/value pair", e);
    } catch (InterruptedException e) {
      // wrap and re-throw
      throw new DatasetReaderException("Interrupted", e);
    }
  }

  private boolean advance() {
    try {
      if (currentReader != null && currentReader.nextKeyValue()) {
        return true;
      } else {
        while (splits.hasNext()) {
          if (currentReader != null) {
            currentReader.close();
          }
          // advance the reader and see if it has records
          InputSplit nextSplit = splits.next();
          this.currentReader = format.createRecordReader(
              nextSplit, attemptContext);
          currentReader.initialize(nextSplit, attemptContext);
          if (currentReader.nextKeyValue()) {
            return true;
          }
        }
        // either no next split or all readers were empty
        return false;
      }
    } catch (IOException e) {
      throw new DatasetIOException("Cannot advance reader", e);
    } catch (InterruptedException e) {
      // wrap and re-throw
      throw new DatasetReaderException("Interrupted", e);
    }
  }

  @Override
  public void close() {
    if (!state.equals(ReaderWriterState.OPEN)) {
      return;
    }

    this.state = ReaderWriterState.CLOSED;


    try {
      if (currentReader != null) {
        currentReader.close();
      }
    } catch (IOException e) {
      throw new DatasetReaderException("Unable to close reader path:" + path, e);
    }

    this.currentReader = null;
    this.hasNext = false;
  }

  @Override
  public boolean isOpen() {
    return (ReaderWriterState.OPEN == state);
  }

  private static TaskAttemptContext newTaskAttemptContext(Configuration conf) {
    try {
      return (TaskAttemptContext) CONTEXT_CONSTRUCTOR.newInstance(conf,
          new TaskAttemptID("", 0, false, 0, 0));
    } catch (InstantiationException e) {
      throw new DatasetException("Cannot instantiate TaskAttemptContext", e);
    } catch (IllegalAccessException e) {
      throw new DatasetException("Cannot instantiate TaskAttemptContext", e);
    } catch (InvocationTargetException e) {
      throw new DatasetException("Cannot instantiate TaskAttemptContext", e);
    }
  }

  @SuppressWarnings("unchecked")
  private static <K, V> FileInputFormat<K, V> newFormatInstance(DatasetDescriptor descriptor) {
    try {
      Class<?> inputFormatClass = Class.forName(
          descriptor.getProperty(INPUT_FORMAT_CLASS_PROP));
      return (FileInputFormat<K, V>) inputFormatClass.newInstance();
    } catch (ClassNotFoundException e) {
      throw new DatasetReaderException("Cannot find InputFormat: " +
          descriptor.getProperty(INPUT_FORMAT_CLASS_PROP), e);
    } catch (InstantiationException e) {
      throw new DatasetReaderException("Cannot instantiate InputFormat: " +
          descriptor.getProperty(INPUT_FORMAT_CLASS_PROP), e);
    } catch (IllegalAccessException e) {
      throw new DatasetReaderException("Cannot instantiate InputFormat: " +
          descriptor.getProperty(INPUT_FORMAT_CLASS_PROP), e);
    }
  }
}
