/**
 * Copyright 2015 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data.kudu;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.kudu.impl.KuduBatchReader;
import org.kitesdk.data.kudu.impl.KuduBatchWriter;
import org.kitesdk.data.spi.AbstractRefinableView;
import org.kitesdk.data.spi.Constraints;
import org.kitesdk.data.spi.InputFormatAccessor;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class KuduView<E> extends AbstractRefinableView<E> implements InputFormatAccessor<E> {
  private final KuduDataset<E> dataset;

  KuduView(KuduDataset<E> dataset, Class<E> type) {
    super(dataset, type);
    this.dataset = dataset;
  }

  private KuduView(KuduView<E> view, Constraints constraints) {
    super(view, constraints);
    this.dataset = view.dataset;
  }

  private KuduView(KuduView<?> view, Schema schema, Class<E> type) {
    super(view, schema, type);
    this.dataset = (KuduDataset<E>) view.dataset.asType(type);
  }

  @Override
  protected AbstractRefinableView<E> filter(Constraints c) {
    return new KuduView<E>(this, c);
  }

  @Override
  protected <T> AbstractRefinableView<T> project(Schema schema, Class<T> type) {
    return new KuduView<T>(this, schema, type);
  }

  @Override
  public InputFormat<E, Void> getInputFormat(Configuration conf) {
    throw new NotImplementedException();
  }

  @Override
  public DatasetReader<E> newReader() {
    return new KuduBatchReader<E>(dataset.getClient(), dataset.getTable(), this);
  }

  @Override
  public DatasetWriter<E> newWriter() {
    return new KuduBatchWriter<E>(dataset.getClient(), dataset.getTable(), this);
  }
}
