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

package org.kitesdk.lang.generics.ruby;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.crunch.types.avro.ReaderWriterFactory;

public class RubyReaderWriterFactory implements ReaderWriterFactory {
  @Override
  public GenericData getData() {
    return RubyData.get();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <D> DatumReader<D> getReader(Schema schema) {
    return RubyData.get().createDatumReader(schema);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <D> DatumWriter<D> getWriter(Schema schema) {
    return RubyData.get().createDatumWriter(schema);
  }
}
