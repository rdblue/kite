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

import org.apache.avro.util.Utf8;
import org.junit.Test;
import org.kitesdk.data.spi.predicates.Range;
import org.kitesdk.data.spi.predicates.Ranges;
import org.kududb.ColumnSchema;
import org.kududb.Type;

public class TestKuduUtil {
  @Test
  public void testUnicodeUpperBound() {
    Range<Utf8> range = Ranges.all();
    ColumnSchema column = new ColumnSchema
        .ColumnSchemaBuilder("test", Type.STRING)
        .build();
    Range<Utf8> closed = KuduUtil.addMissingEndpoints(column, range);
    System.err.println("Lower: " + closed.lowerEndpoint());
    System.err.println("Upper: " + closed.upperEndpoint());
  }
}
