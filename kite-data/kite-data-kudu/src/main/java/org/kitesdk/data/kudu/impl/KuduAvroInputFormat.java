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

package org.kitesdk.data.kudu.impl;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.kitesdk.compat.Hadoop;
import org.kitesdk.data.DatasetOperationException;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.kudu.KuduUtil;
import org.kitesdk.data.kudu.KuduView;
import org.kitesdk.data.spi.Constraints;
import org.kitesdk.data.spi.FilteredRecordReader;
import org.kitesdk.data.spi.predicates.Range;
import org.kududb.ColumnSchema;
import org.kududb.client.KuduTable;
import org.kududb.client.RowResult;
import org.kududb.mapreduce.KuduTableInputFormat;
import org.kududb.mapreduce.KuduTableMapReduceUtil;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class KuduAvroInputFormat<E> extends InputFormat<E, Void> {
  public static final String RESIDUALS = "kite.kudu.residual-predicates";

  private final KuduView<E> view;
  private final Constraints residuals;
  private final KuduTableInputFormat kuduFormat;

  public KuduAvroInputFormat(KuduView<E> view, Configuration conf) {
    this.view = view;
    this.residuals = Constraints.fromQueryMap(view.getSchema(),
        view.getDataset().getDescriptor().getPartitionStrategy(),
        KuduUtil.stringToMap(conf.get(RESIDUALS)));
    this.kuduFormat = new KuduTableInputFormat();
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    return kuduFormat.getSplits(context);
  }

  @Override
  public RecordReader<E, Void> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    RecordReader<E, Void> reader = new KuduAvroRecordReader<>(
        kuduFormat.createRecordReader(split, context),
        view.getType(), view.getSchema());
    if (residuals.isUnbounded()) {
      return reader;
    }
    return new FilteredRecordReader<E>(
        reader, residuals, view.getAccessor());
  }

  private static class KuduAvroRecordReader<E> extends RecordReader<E, Void> {
    private final RecordReader<NullWritable, RowResult> wrapped;
    private final Class<E> recordClass;
    private final Schema schema;

    public KuduAvroRecordReader(RecordReader<NullWritable, RowResult> wrapped,
                                Class<E> recordClass, Schema schema) {
      this.wrapped = wrapped;
      this.recordClass = recordClass;
      this.schema = schema;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      wrapped.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return wrapped.nextKeyValue();
    }

    @Override
    public E getCurrentKey() throws IOException, InterruptedException {
      return KuduUtil.makeRecord(wrapped.getCurrentValue(), recordClass, schema);
    }

    @Override
    public Void getCurrentValue() throws IOException, InterruptedException {
      return null;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return wrapped.getProgress();
    }

    @Override
    public void close() throws IOException {
      wrapped.close();
    }
  }

  public static void configure(String master, KuduTable table, Schema schema,
                               Constraints constraints, Configuration conf) {
    PartitionStrategy strategy = constraints.getPartitionStrategy();
    org.kududb.Schema kuduSchema = table.getSchema();

    Job fakeJob = Hadoop.Job.newInstance.invoke(new Configuration(false));
    KuduTableMapReduceUtil.TableInputFormatConfigurator configurator =
        new KuduTableMapReduceUtil.TableInputFormatConfigurator(
            fakeJob, table.getName(), columns(schema), master);

    // push down constraints if possible
    Constraints leftOver = new Constraints(schema, strategy);
    for (Map.Entry<String, Predicate> constraint : constraints.getAll().entrySet()) {
      if (constraint.getValue() instanceof Range) {
        ColumnSchema column = kuduSchema.getColumn(constraint.getKey());
        configurator.addColumnRangePredicate(KuduUtil.toKuduPredicate(
            (Range<?>) constraint.getValue(), column));
      } else {
        leftOver = leftOver.with(constraint.getKey(), constraint.getValue());
      }
    }

    try {
      configurator.configure();
    } catch (IOException e) {
      throw new DatasetOperationException(
          "MR configuration failed for table: " + table, e);
    }

    // copy all of the settings into the real Configuration
    for (Map.Entry<String, String> entry : fakeJob.getConfiguration()) {
      conf.set(entry.getKey(), entry.getValue());
    }

    // add the constraints that weren't pushed down to Kudu
    conf.set(KuduAvroInputFormat.RESIDUALS,
        KuduUtil.mapToString(leftOver.toQueryMap()));
  }

  private static final Joiner COMMA = Joiner.on(',');

  private static String columns(Schema schema) {
    List<String> columns = Lists.newArrayList();
    for (Schema.Field field : schema.getFields()) {
      columns.add(field.name());
    }
    return COMMA.join(columns);
  }
}
