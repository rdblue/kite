package org.kitesdk.data.kudu; /**
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
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.spi.AbstractDatasetRepository;
import org.kududb.client.KuduClient;

import java.net.URI;
import java.util.Collection;

public class KuduDatasetRepository extends AbstractDatasetRepository {
  private KuduClient kuduClient;
  private final URI repositoryUri;

  KuduDatasetRepository(KuduClient kuduClient, URI repositoryUri) {
    this.kuduClient = kuduClient;
    this.repositoryUri = repositoryUri;
  }

  @Override
  public <E> Dataset<E> load(String namespace, String name, Class<E> type) {
    return null;
  }

  @Override
  public <E> Dataset<E> create(String namespace, String name, DatasetDescriptor descriptor, Class<E> type) {
    return null;
  }

  @Override
  public <E> Dataset<E> update(String namespace, String name, DatasetDescriptor descriptor, Class<E> type) {
    return null;
  }

  @Override
  public boolean delete(String namespace, String name) {
    return false;
  }

  @Override
  public boolean exists(String namespace, String name) {
    return false;
  }

  @Override
  public Collection<String> namespaces() {
    return null;
  }

  @Override
  public Collection<String> datasets(String namespace) {
    return null;
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
