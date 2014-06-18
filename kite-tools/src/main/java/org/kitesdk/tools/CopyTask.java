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

package org.kitesdk.tools;

import java.net.URI;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.hadoop.conf.Configured;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.View;
import org.kitesdk.data.crunch.CrunchDatasets;

public class CopyTask<E> extends Configured {

  private static final String LOCAL_FS_SCHEME = "file";

  private final View<E> from;
  private final View<E> to;
  private final Class<E> entityClass;

  public CopyTask(View<E> from, View<E> to, Class<E> entityClass) {
    this.from = from;
    this.to = to;
    this.entityClass = entityClass;
  }

  public PipelineResult run() {
    boolean runInParallel = true;
    if (isLocal(from.getDataset()) || isLocal(to.getDataset())) {
      runInParallel = false;
    }

    // TODO: Add reduce phase and allow control over the number of reducers
    Pipeline pipeline = runInParallel ?
        new MRPipeline(getClass(), getConf()) : MemPipeline.getInstance();

    pipeline.write(
        pipeline.read(CrunchDatasets.asSource(from, entityClass)),
        CrunchDatasets.asTarget(to));

    return pipeline.done();
  }

  private static boolean isLocal(Dataset<?> dataset) {
    URI location = dataset.getDescriptor().getLocation();
    return (location != null) && LOCAL_FS_SCHEME.equals(location.getScheme());
  }
}
