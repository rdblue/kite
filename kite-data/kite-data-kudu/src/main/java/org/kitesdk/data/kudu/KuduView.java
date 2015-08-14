package org.kitesdk.data.kudu;

import org.apache.avro.Schema;
import org.kitesdk.compat.Hadoop;
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
