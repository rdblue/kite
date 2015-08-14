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

package org.kitesdk.data.kudu;

import org.apache.avro.Schema;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.spi.AbstractRefinableView;
import org.kitesdk.data.spi.Constraints;
import org.kitesdk.data.spi.InputFormatAccessor;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.conf.Configuration;

public class KuduView<E>  extends AbstractRefinableView<E>
    implements InputFormatAccessor<E> {
  protected KuduView(Dataset<E> dataset, Class<E> type) {
    super(dataset, type);
  }

  @Override
  protected AbstractRefinableView<E> filter(Constraints c) {
    return null;
  }

  @Override
  protected <T> AbstractRefinableView<T> project(Schema schema, Class<T> type) {
    return null;
  }

  @Override
  public DatasetReader<E> newReader() {
    return null;
  }

  @Override
  public DatasetWriter<E> newWriter() {
    return null;
  }

  @Override
  public InputFormat<E, Void> getInputFormat(Configuration conf) {
    return null;
  }
}
