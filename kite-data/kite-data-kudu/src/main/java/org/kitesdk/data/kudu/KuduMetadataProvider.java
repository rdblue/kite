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

import java.io.IOException;
import java.util.Collection;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetExistsException;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.DatasetOperationException;
import org.kitesdk.data.kudu.impl.KuduSchemaConverter;
import org.kitesdk.data.spi.AbstractMetadataProvider;
import org.kududb.Schema;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduTable;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class KuduMetadataProvider extends AbstractMetadataProvider {
  private KuduClient kuduClient;

  KuduMetadataProvider(KuduClient kuduClient) {
    this.kuduClient = kuduClient;
  }

  @Override
  public DatasetDescriptor load(String namespace, String name) {
    try {
      if (!exists(name)) {
        throw new DatasetNotFoundException(
            String.format("Table [%s] does not exist", name));
      }
      KuduTable kuduTable = kuduClient.openTable(name);

      DatasetDescriptor.Builder builder = new DatasetDescriptor.Builder();

      builder.schema(KuduSchemaConverter.schemaFor(
          kuduTable.getName(), kuduTable.getSchema().getColumns()));
      builder.partitionStrategy(KuduSchemaConverter.strategyFor(
          kuduTable.getSchema().getColumns()));

      return builder.build();
    } catch (Exception e) {
      throw kuduException(e);
    }
  }

  public DatasetDescriptor load(String name) {
    return load(null, name);
  }

  @Override
  public DatasetDescriptor create(String namespace, String name,
      DatasetDescriptor descriptor) {
    Preconditions.checkArgument(descriptor.isPartitioned(),
        "Cannot use Kudu without a partition strategy");

    Schema kuduSchema = KuduSchemaConverter
        .convertSchema(descriptor.getSchema(), descriptor.getPartitionStrategy());
    try {
      if (!exists(name)) {
        kuduClient.createTable(name, kuduSchema);
        return descriptor;
      } else {
        throw new DatasetExistsException(
            String.format("Table [%s] already exists", name));
      }
    } catch (Exception e) {
      throw kuduException(e);
    }
  }

  public DatasetDescriptor create(String name, DatasetDescriptor descriptor) {
    return create(null, name, descriptor);
  }

  @Override
  public DatasetDescriptor update(String namespace, String name,
      DatasetDescriptor descriptor) {
    throw new UnsupportedOperationException("Kudu doesn't support update");
  }

  public DatasetDescriptor update(String name, DatasetDescriptor descriptor) {
    return update(null, name, descriptor);
  }

  @Override
  public boolean delete(String namespace, String name) {
    try {
      kuduClient.deleteTable(name);
      return true;
    } catch (Exception e) {
      throw kuduException(e);
    }
  }

  public boolean delete(String name) {
    return delete(null, name);
  }

  @Override
  public boolean exists(String namespace, String name) {
    try {
      return kuduClient.tableExists(name);
    } catch (Exception e) {
      throw kuduException(e);
    }
  }

  public boolean exists(String name) {
    return exists(null, name);
  }

  @Override
  public Collection<String> namespaces() {
    return ImmutableList.of("default");
  }

  @Override
  public Collection<String> datasets(String namespace) {
    try {
      return kuduClient.getTablesList().getTablesList();
    } catch (Exception e) {
      throw kuduException(e);
    }
  }

  public Collection<String> datasets() {
    return datasets("default");
  }

  /**
   * Helper method to throw a DatasetIOException for when Kudu misbehaves
   * 
   * @param e
   *          Exception from Kudu client
   */
  private DatasetOperationException kuduException(Exception e) {
    return new DatasetOperationException(
        "Could not communicate with kudu master", e);
  }
}
