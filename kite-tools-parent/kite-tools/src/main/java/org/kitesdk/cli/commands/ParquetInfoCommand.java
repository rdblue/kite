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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.kitesdk.data.Formats;
import org.kitesdk.data.spi.filesystem.FileSystemDataset;
import org.slf4j.Logger;
import java.io.IOException;
import java.util.List;

@Parameters(commandDescription = "Get info about Parquet layout Dataset")
public class ParquetInfoCommand extends BaseDatasetCommand {

  @Parameter(description = "<dataset>")
  List<String> datasets;

  public ParquetInfoCommand(Logger console) {
    super(console);
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value={"NP_GUARANTEED_DEREF", "NP_NULL_ON_SOME_PATH"},
      justification="Null case checked by precondition")
  public int run() throws IOException {
    Preconditions.checkArgument(
        datasets != null && datasets.size() == 1, "A dataset is required");

    FileSystemDataset<?> dataset = (FileSystemDataset) load(datasets.get(0))
        .getDataset();
    Preconditions.checkArgument(
        dataset.getDescriptor().getFormat().equals(Formats.PARQUET),
        "Not a Parquet dataset: " + dataset.getUri());
    FileSystem fs = dataset.getFileSystem();

    int splitRowGroupFiles = 0;
    int totalSplitRowGroups = 0;
    long totalRemoteBytes = 0;

    for (Path file : dataset.pathIterator()) {
      FileStatus stat = fs.getFileStatus(file);
      long blockSize = stat.getBlockSize();

      int rowGroupNumber = 0;
      int splitRowGroups = 0;
      long remoteBytes = 0;

      ParquetMetadata footer = ParquetFileReader.readFooter(getConf(), file);
      for (BlockMetaData block : footer.getBlocks()) {
        long start = Long.MAX_VALUE;
        long end = Long.MIN_VALUE;
        for (ColumnChunkMetaData chunk : block.getColumns()) {
          start = Math.min(start, chunk.getStartingPos());
          end = Math.max(end, chunk.getStartingPos() + chunk.getTotalSize());
        }

        long startBlock = start / blockSize;
        long endBlock = end / blockSize;
        if (startBlock != endBlock) {
          remoteBytes += Math.min(
              blockSize - (start % blockSize),
              end % blockSize);
          splitRowGroups += 1;
          console.info(
              "Row group spans two blocks: {}-{}: start: {} end: {} remote: {}",
              new Object[] {file.getName(), rowGroupNumber, start, end, remoteBytes});
        }

        rowGroupNumber += 1;
      }

      if (splitRowGroups > 0) {
        splitRowGroupFiles += 1;
        console.info("File {}: {} split row groups, {} remote bytes",
            new Object[] {file, splitRowGroups, remoteBytes});
      }

      totalSplitRowGroups += splitRowGroups;
      totalRemoteBytes += remoteBytes;
    }

    if (splitRowGroupFiles > 0) {
      console.info("Totals: {} split row groups in {} files, {} remote bytes",
          new Object[] {totalSplitRowGroups, splitRowGroupFiles, totalRemoteBytes});
    } else {
      console.info("No split row groups!");
    }

    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
    );
  }
}
