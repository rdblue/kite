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
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.kitesdk.compat.DynFields;
import org.slf4j.Logger;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

@Parameters(commandDescription = "Show the schema for a Dataset")
public class MergeParquetCommand extends BaseDatasetCommand {

  @Parameter(description = "<files>")
  List<String> files;

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="UWF_NULL_FIELD",
      justification = "Field set by JCommander")
  @Parameter(
      names={"-o", "--output"},
      description="Save combined Parquet file to path",
      required = true)
  String outputPath = null;

  @Parameter(
      names={"-b", "--block-size"},
      description="HDFS block size for the output file",
      required = true)
  long blockSize = -1;

  @Parameter(
      names={"-p", "--max-padding"},
      description="Max padding size for the output file")
  int maxPadding = 8*1024*1024;

  @Parameter(
      names={"--pad-local-files"},
      description="Pad local files",
      hidden = true)
  boolean padLocalFiles = false;

  @DynamicParameter(names = {"--set", "--property"},
      description = "Add a property pair: prop.name=value")
  Map<String, String> properties = new HashMap<String, String>();

  public MergeParquetCommand(Logger console) {
    super(console);
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value={"NP_GUARANTEED_DEREF", "NP_NULL_ON_SOME_PATH"},
      justification="Null case checked by precondition")
  public int run() throws IOException {
    Preconditions.checkArgument(
        files != null && files.size() > 0, "Not enough files to merge");

    if (properties != null) {
      for (Map.Entry<String, String> entry : properties.entrySet()) {
        getConf().set(entry.getKey(), entry.getValue());
      }
    }

    ParquetOutputFormat.setMaxPaddingSize(getConf(), maxPadding);

    if (blockSize > 0) {
      getConf().setLong("dfs.block.size", blockSize);
      getConf().setLong("dfs.blocksize", blockSize);
      getConf().setLong("dfs.blockSize", blockSize);
    }

    if (padLocalFiles) {
      if (blockSize < 0) {
        blockSize = 128*1024*1024;
      }
      getConf().setLong("fs.local.block.size", blockSize);
      getConf().setBoolean("fs.file.impl.disable.cache", true);
      HashSet<String> BLOCK_FS_SCHEMES = new DynFields
          .Builder<HashSet<String>>("BLOCK_FS_SCHEMES")
          .fromHidden(ParquetFileWriter.class)
          .buildStatic()
          .get();
      BLOCK_FS_SCHEMES.add("file");
    }

    ParquetMetadata footer = ParquetFileReader.readFooter(
        getConf(), qualifiedPath(files.get(0)));
    MessageType parquetSchema = footer.getFileMetaData().getSchema();

    boolean threw = true;
    try {
      ParquetFileWriter writer = new ParquetFileWriter(
          getConf(), parquetSchema, qualifiedPath(outputPath),
          ParquetFileWriter.Mode.OVERWRITE, blockSize, maxPadding);

      writer.start();
      for (String file : files) {
        writer.appendFile(getConf(), qualifiedPath(file));
      }
      writer.end(Maps.<String, String>newHashMap());
      threw = false;
    } finally {
      if (threw) {

      }
    }

    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Print the schema for dataset \"users\" to standard out:",
        "users",
        "# Print the schema for a dataset URI to standard out:",
        "dataset:hbase:zk1,zk2/users",
        "# Save the schema for dataset \"users\" to user.avsc:",
        "users -o user.avsc"
    );
  }
}
