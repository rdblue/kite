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
import com.facebook.fb303.FacebookService;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.AccessController;
import java.security.CodeSource;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.crunch.util.DistCache;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.thrift.TUGIContainingTransport;
import org.apache.thrift.TException;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.hcatalog.HCatalogDatasetRepository;
import org.kitesdk.tools.CopyTask;
import org.slf4j.Logger;

@Parameters(commandDescription="Copy records from one Dataset to another")
public class CopyCommand extends BaseDatasetCommand {

  public CopyCommand(Logger console) {
    super(console);
  }

  @Parameter(description="<source dataset> <destination dataset>")
  List<String> datasets;

  @Override
  public int run() throws IOException {
    Preconditions.checkArgument(datasets != null && !datasets.isEmpty(),
        "Dataset names are required");
    Preconditions.checkArgument(datasets.size() == 2,
        "Cannot copy multiple datasets");

    DatasetRepository repo = getDatasetRepository();
    Dataset<GenericData.Record> source = repo.load(datasets.get(0));
    Dataset<GenericData.Record> dest = repo.load(datasets.get(1));

    Class[] classes = new Class<?>[]{
        HiveConf.class,               // hive-exec
//        NoSuchObjectException.class,  // hive-metastore
  //      TException.class,             // thrift (for Hive)
    //    TUGIContainingTransport.class,
      //  FacebookService.class,        // fb303 (for Hive)
        AvroKeyInputFormat.class      // avro-mapred
    };
    for (Class<?> requiredClass : classes) {
      ProtectionDomain domain = AccessController.doPrivileged(
          new GetProtectionDomain(requiredClass));
      CodeSource codeSource = domain.getCodeSource();
      File jar;
      if (codeSource != null) {
        try {
          jar = new File(codeSource.getLocation().toURI());
        } catch (URISyntaxException e) {
          throw new DatasetException(
              "Cannot locate " + requiredClass.getName() + " jar", e);
        }
      } else {
        throw new DatasetException(
            "Cannot locate " + requiredClass.getName() + " jar");
      }
      DistCache.addJarDirToDistributedCache(getConf(), jar.getParent());
    }

    CopyTask task = new CopyTask<GenericData.Record>(
        source, dest, GenericData.Record.class);
    task.setConf(getConf());

    return task.run().succeeded() ? 0 : 1;
  }

  /**
   * A PrivilegedAction that gets the ProtectionDomain for a dependency class.
   *
   * Using a PrivilegedAction to retrieve the domain allows security policies
   * to enable Kite to do this, but exclude client code.
   */
  public static class GetProtectionDomain
      implements PrivilegedAction<ProtectionDomain> {
    private final Class<?> requiredClass;

    public GetProtectionDomain(Class<?> requiredClass) {
      this.requiredClass = requiredClass;
    }

    @Override
    public ProtectionDomain run() {
      return requiredClass.getProtectionDomain();
    }
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Copy the contents of movies to movies2",
        "movies movies2"
    );
  }
}
