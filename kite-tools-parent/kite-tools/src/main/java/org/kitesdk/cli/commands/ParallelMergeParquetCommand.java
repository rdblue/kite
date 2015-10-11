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

package org.kitesdk.cli.commands;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.Encoder;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.schema.MessageType;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.api.jdo.JDOAdapter;
import org.datanucleus.store.rdbms.query.SQLQuery;
import org.kitesdk.compat.Hadoop;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetOperationException;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.PartitionView;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.filesystem.FileSystemDataset;
import org.kitesdk.data.spi.filesystem.FileSystemPartitionView;
import org.kitesdk.data.spi.filesystem.PathFilters;
import org.kitesdk.tools.TaskUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.jdo.JDOHelper;
import javax.transaction.Transaction;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Parameters(commandDescription = "Show the schema for a Dataset")
public class ParallelMergeParquetCommand extends BaseDatasetCommand {

  @Parameter(description = "<in-dataset> <out-dataset>")
  List<String> datasets;

  @Parameter(
      names={"-b", "--block-size"},
      description="HDFS block size for the output file",
      required = true)
  int blockSize = 128*1024*1024;

  @Parameter(
      names={"-p", "--max-padding"},
      description="Max padding size for the output file")
  int maxPadding = 8*1024*1024;

  @DynamicParameter(names = {"--set", "--property"},
      description = "Add a property pair: prop.name=value")
  Map<String, String> properties = new HashMap<String, String>();

  public ParallelMergeParquetCommand(Logger console) {
    super(console);
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value={"NP_GUARANTEED_DEREF", "NP_NULL_ON_SOME_PATH"},
      justification="Null case checked by precondition")
  public int run() throws IOException {
    Preconditions.checkArgument(
        datasets != null && datasets.size() == 2,
        "Source and target datasets are required");

    View<GenericRecord> source = load(datasets.get(0));
    Dataset<GenericRecord> target = load(datasets.get(1)).getDataset();

    if (properties != null) {
      for (Map.Entry<String, String> entry : properties.entrySet()) {
        getConf().set(entry.getKey(), entry.getValue());
      }
    }

    Job job = Hadoop.Job.newInstance.invoke(getConf());
    Configuration conf = job.getConfiguration();

    if (blockSize > 0) {
      ParquetOutputFormat.setBlockSize(job, blockSize);
      conf.setLong("dfs.block.size", blockSize);
      conf.setLong("dfs.blocksize", blockSize);
      conf.setLong("dfs.blockSize", blockSize);
    }

    ParquetOutputFormat.setMaxPaddingSize(job, maxPadding);
    job.setJarByClass(ParallelMergeParquetCommand.class);
    job.setInputFormatClass(PartitionInputFormat.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setMapperClass(MergeMapper.class);
    job.setNumReduceTasks(0);

    conf.set("input.uri", source.getUri().toString());
    conf.set("output.uri", target.getUri().toString());

    Class<?> fb303Class, thriftClass;
    try {
      // attempt to use libfb303 and libthrift 0.9.2 when async was added
      fb303Class = Class.forName(
          "com.facebook.fb303.FacebookService.AsyncProcessor");
      thriftClass = Class.forName(
          "org.apache.thrift.TBaseAsyncProcessor");
    } catch (ClassNotFoundException e) {
      try {
        // fallback to 0.9.0 or earlier
        fb303Class = Class.forName(
            "com.facebook.fb303.FacebookBase");
        thriftClass = Class.forName(
            "org.apache.thrift.TBase");
      } catch (ClassNotFoundException real) {
        throw new DatasetOperationException(
            "Cannot find thrift dependencies", real);
      }
    }

    TaskUtil.configure(conf)
        .addJarForClass(Encoder.class) // commons-codec
        .addJarForClass(Log.class) // commons-logging
        .addJarForClass(CompressorInputStream.class) // commons-compress
        .addJarForClass(ApiAdapter.class) // datanucleus-core
        .addJarForClass(JDOAdapter.class) // datanucleus-api-jdo
        .addJarForClass(SQLQuery.class) // datanucleus-rdbms
        .addJarForClass(JDOHelper.class) // jdo-api
        .addJarForClass(Transaction.class) // jta
        .addJarForClass(fb303Class) // libfb303
        .addJarForClass(thriftClass) // libthrift
        .addJarForClass(HiveOutputFormat.class) // ???
        .addJarForClass(HiveMetaStore.class) // hive-metastore
        .addJarForClass(HiveConf.class); // hive-exec

    try {
      job.waitForCompletion(true);
    } catch (InterruptedException e) {
      throw new DatasetOperationException("Merge job failed", e);
    } catch (ClassNotFoundException e) {
      throw new DatasetOperationException("Merge job failed", e);
    }

    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
    );
  }

  public static class MergeMapper extends Mapper<String, Void, Void, Void> {
    private static final Logger LOG = LoggerFactory.getLogger(MergeMapper.class);

    @Override
    protected void map(String path, Void _, Context context)
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();

      FileSystemDataset<GenericRecord> dataset = Datasets.load(
          conf.get("input.uri"));

      Path inPartition = new Path(path);
      FileSystem fs = inPartition.getFileSystem(conf);
      FileStatus[] stats = fs.listStatus(inPartition, PathFilters.notHidden());

      if (stats.length < 1) {
        return;
      }

      FileStatus first = stats[0];

      // sort the files by size descending to ensure the any files with partial
      // row groups are written last
      Arrays.sort(stats, new Comparator<FileStatus>() {
        @Override
        public int compare(FileStatus o1, FileStatus o2) {
          return Long.compare(o2.getLen(), o1.getLen());
        }
      });

      LOG.info("Merging {} parquet files in {}", stats.length, path);

      FileSystemPartitionView<?> view = dataset.getPartitionView(inPartition);
      String relative = view.getRelativeLocation().toString();
      LOG.info("Using relative path: {}", relative);

      FileSystemDataset<GenericRecord> out = Datasets.load(
          conf.get("output.uri"));

      Path outPath = new Path(new Path(out.getDirectory(), relative),
          first.getPath().getName());
      LOG.info("Final output path: {}", outPath.toString());
      Path dotFile = new Path(outPath.getParent(),
          "." + outPath.getName() + ".tmp");
      LOG.info("Temporary output path: {}", dotFile.toString());
      FileSystem outFS = out.getFileSystem();

      MessageType schema = ParquetFileReader.readFooter(conf, first.getPath())
          .getFileMetaData()
          .getSchema();

      boolean threw = true;
      try {

        ParquetFileWriter writer = new ParquetFileWriter(
            conf, schema, dotFile, ParquetFileWriter.Mode.OVERWRITE,
            ParquetOutputFormat.getBlockSize(context),
            conf.getInt("parquet.writer.max-padding", 8388608));

        writer.start();
        for (FileStatus stat : stats) {
          LOG.info("Appending blocks from: {}", stat.getPath().toString());
          writer.appendFile(context.getConfiguration(), stat.getPath());
        }
        writer.end(Maps.<String, String>newHashMap());

        threw = false;

      } finally {
        if (threw) {
          LOG.info("Merge caused an exception: Deleting temporary data: {}",
              dotFile.toString());
          outFS.delete(dotFile);
        } else {
          LOG.info("Committing {} as {}",
              dotFile.toString(), outPath.toString());
          outFS.rename(dotFile, outPath);
        }
      }
    }
  }

  public static class PartitionInputSplit extends InputSplit implements Writable {
    private String partition;

    public PartitionInputSplit() {
    }

    public PartitionInputSplit(String partition) {
      this.partition = partition;
    }

    public String getPartition() {
      return partition;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
      return 1;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      return new String[0];
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(partition);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.partition = in.readUTF();
    }
  }

  public static class PartitionInputFormat extends InputFormat<String, Void> {
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
      View<?> view = Datasets.load(context.getConfiguration().get("input.uri"));
      List<InputSplit> splits = Lists.newArrayList();
      for (PartitionView<?> partition : view.getCoveringPartitions()) {
        splits.add(new PartitionInputSplit(partition.getLocation().toString()));
      }
      return splits;
    }

    @Override
    public RecordReader<String, Void> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      return new RecordReader<String, Void>() {
        private String path = null;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
          PartitionInputSplit partition = (PartitionInputSplit) split;
          this.path = partition.getPartition();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
          return (path != null);
        }

        @Override
        public String getCurrentKey() throws IOException, InterruptedException {
          String current = path;
          this.path = null; // only return it once
          return current;
        }

        @Override
        public Void getCurrentValue() throws IOException, InterruptedException {
          return null;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
          return 0;
        }

        @Override
        public void close() throws IOException {
          this.path = null;
        }
      };
    }
  }

}
