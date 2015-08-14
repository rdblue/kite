package org.kitesdk.data.kudu;

import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Key;
import org.kitesdk.data.RandomAccessDataset;
import org.kitesdk.data.RefinableView;
import org.kitesdk.data.spi.AbstractDataset;
import org.kitesdk.data.spi.AbstractRefinableView;
import org.kitesdk.data.spi.Constraints;

import java.net.URI;

public class KuduDataset<E> extends AbstractDataset<E> implements
    RandomAccessDataset<E> {
  private final String namespace;
  private final String name;
  //private final DatasetDescriptor descriptor;

  public KuduDataset(String namespace, String name,
      DatasetDescriptor descriptor, Class<E> type) {
    super(type, descriptor.getSchema());
    Preconditions.checkArgument(
        IndexedRecord.class.isAssignableFrom(type) || type == Object.class,
        "HBase only supports the generic and specific data models. The entity"
            + " type must implement IndexedRecord");
    this.namespace = namespace;
    this.name = name;
    //this.descriptor = new KuduView<E>(this, type);
  }

  @Override
  protected RefinableView<E> asRefinableView() {
    return null;
  }

  @Override
  public AbstractRefinableView<E> filter(Constraints c) {
    return null;
  }

  @Override
  public E get(Key key) {
    return null;
  }

  @Override
  public boolean put(E entity) {
    return false;
  }

  @Override
  public long increment(Key key, String fieldName, long amount) {
    return 0;
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
    return null;
  }

  @Override
  public String getNamespace() {
    return null;
  }

  @Override
  public DatasetDescriptor getDescriptor() {
    return null;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public URI getUri() {
    return null;
  }
}
