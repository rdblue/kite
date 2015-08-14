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
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.RandomAccessDataset;
import org.kitesdk.data.spi.AbstractDatasetRepository;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduTable;

import java.net.URI;
import java.util.Collection;

public class KuduDatasetRepository extends AbstractDatasetRepository {
  private KuduClient kuduClient;
  private KuduTable kuduTable;
  private KuduMetadataProvider metadataProvider;
  private final URI repositoryUri;

  KuduDatasetRepository(KuduClient kuduClient, URI repositoryUri) {
    this.kuduClient = kuduClient;
    this.repositoryUri = repositoryUri;
    this.metadataProvider = new KuduMetadataProvider(kuduClient);
  }

  @Override
  public <E> RandomAccessDataset<E> load(String namespace, String name, Class<E> type) {
    return new KuduDataset<E>(namespace, name, kuduClient, kuduTable, metadataProvider.load(name), repositoryUri, type);
  }

  @Override
  public <E> RandomAccessDataset<E> create(String namespace, String name, DatasetDescriptor descriptor, Class<E> type) {
    metadataProvider.create(name, descriptor);
    return new KuduDataset<E>(namespace, name, kuduClient, kuduTable, descriptor, repositoryUri, type);
  }

  @Override
  public <E> RandomAccessDataset<E> update(String namespace, String name, DatasetDescriptor descriptor, Class<E> type) {
    // this will throw a not implemented exception
    return new KuduDataset<E>(namespace, name, kuduClient, kuduTable, metadataProvider.update(name, descriptor), repositoryUri, type);
  }

  @Override
  public boolean delete(String namespace, String name) {
    return metadataProvider.delete(name);
  }

  @Override
  public boolean exists(String namespace, String name) {
    return metadataProvider.exists(name);
  }

  @Override
  public Collection<String> namespaces() {
    return metadataProvider.namespaces();
  }

  @Override
  public Collection<String> datasets(String namespace) {
    return metadataProvider.datasets();
  }

  @Override
  public URI getUri() {
    return repositoryUri;
  }

  public static class Builder {
    private String master;

    public Builder master(String master) {
      this.master = master;
      return this;
    }

    public KuduDatasetRepository build() {
      KuduClient client = new KuduClient.KuduClientBuilder(master).build();
      return new KuduDatasetRepository(client, getDatasetUri(master));
    }

    private URI getDatasetUri(String master) {
      return URI.create("repo:kudu:" + master);
    }
  }
}
