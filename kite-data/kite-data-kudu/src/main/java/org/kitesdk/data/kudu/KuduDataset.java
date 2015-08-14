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

import com.google.common.base.Preconditions;
import org.apache.avro.generic.IndexedRecord;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetOperationException;
import org.kitesdk.data.DatasetRecordException;
import org.kitesdk.data.Key;
import org.kitesdk.data.RandomAccessDataset;
import org.kitesdk.data.RefinableView;
import org.kitesdk.data.spi.AbstractDataset;
import org.kitesdk.data.spi.AbstractRefinableView;
import org.kitesdk.data.spi.Constraints;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduSession;
import org.kududb.client.KuduTable;
import org.kududb.client.OperationResponse;
import org.kududb.client.SessionConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class KuduDataset<E> extends AbstractDataset<E>
    implements RandomAccessDataset<E> {
  private final String namespace;
  private final String name;
  private final KuduView<E> unbounded;
  private final KuduSession session;
  private final DatasetDescriptor descriptor;
  private final URI uri;
  private final KuduClient kuduClient;
  private final KuduTable kuduTable;
  private static final Logger LOG = LoggerFactory.getLogger(KuduDataset.class);

  public KuduDataset(String namespace, String name, KuduClient kuduClient,
      KuduTable kuduTable, DatasetDescriptor descriptor, URI uri,
      Class<E> type) {
    super(type, descriptor.getSchema());
    Preconditions.checkArgument(
        IndexedRecord.class.isAssignableFrom(type) || type == Object.class,
        "HBase only supports the generic and specific data models. The entity"
            + " type must implement IndexedRecord");
    this.namespace = namespace;
    this.name = name;
    this.descriptor = descriptor;
    this.uri = uri;
    this.kuduClient = kuduClient;
    this.session = kuduClient.newSession();
    this.session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
    this.kuduTable = kuduTable;
    this.unbounded = new KuduView<E>(this, type);
  }

  @Override
  protected RefinableView<E> asRefinableView() {
    return unbounded;
  }

  @Override
  public AbstractRefinableView<E> filter(Constraints c) {
    return unbounded.filter(c);
  }

  @Override
  public E get(Key key) {
    return null;
  }

  @Override
  public boolean put(E entity) {
    Preconditions.checkState(!session.isClosed(), "Dataset session is closed");

    if (!unbounded.includes(entity)) {
      throw new DatasetRecordException(String
          .format("Cannot write '%s': not in %s", entity, unbounded.getUri()));
    }

    try {
      OperationResponse response = session.apply(KuduUtil
          .buildInsert(entity, kuduTable, unbounded.getAccessor()));

      if (response.hasRowError()) {
        throw new DatasetRecordException(response.getRowError().toString());
      }
      return true;
    } catch (Exception e) {
      throw new DatasetOperationException("Write failed", e);
    }
  }

  @Override
  public long increment(Key key, String fieldName, long amount) {
    throw new UnsupportedOperationException(
        "increment not supported in kudu currently");
  }

  @Override
  public void delete(Key key) {

  }

  @Override
  public boolean delete(E entity) {
    return false;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getNamespace() {
    return namespace;
  }

  @Override
  public DatasetDescriptor getDescriptor() {
    return descriptor;
  }

  @Override
  public boolean isEmpty() {
    return unbounded.isEmpty();
  }

  @Override
  public URI getUri() {
    return uri;
  }
}
