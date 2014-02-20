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

package org.kitesdk.data.spi;

import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;
import com.google.common.collect.Sets;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;

public class RangeRecord implements IndexedRecord {
  private final Schema itemSchema;
  private Range range = null;
  private Comparable lower = null;
  private Comparable upper = null;

  public RangeRecord(Schema itemSchema) {
    this.itemSchema = itemSchema;
  }

  public void setRange(Range range) {
    this.range = range;
  }

  public Range getRange() {
    return range;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void put(int i, Object v) {
    switch (i) {
      case 0:
        this.range = null; // clear out the last range
        if (v instanceof Utf8) {
          this.lower = v.toString();
        } else {
          this.lower = (Comparable) v;
        }
        break;
      case 1:
        if (v != null) {
          if ((Boolean) v) { // is bound closed?
            this.range = Ranges.atLeast(lower);
          } else {
            this.range = Ranges.greaterThan(lower);
          }
        }
        break;
      case 2:
        if (v instanceof Utf8) {
          this.upper = v.toString();
        } else {
          this.upper = (Comparable) v;
        }
        break;
      case 3:
        if (v != null) {
          Range upperRange;
          if ((Boolean) v) { // is bound closed?
            upperRange = Ranges.atMost(upper);
          } else {
            upperRange = Ranges.lessThan(upper);
          }
          if (range != null) {
            this.range = range.intersection(upperRange);
          } else {
            this.range = upperRange;
          }
        }
        break;
      default:
        throw new IllegalArgumentException("Bad field index");
    }
  }

  @Override
  public Object get(int i) {
    switch (i) {
      case 0:
        return range != null && range.hasLowerBound() ?
            range.lowerEndpoint() : null;
      case 1:
        return range != null && range.hasLowerBound() ?
            range.lowerBoundType() == BoundType.CLOSED : null;
      case 2:
        return range != null && range.hasUpperBound() ?
            range.upperEndpoint() : null;
      case 3:
        return range != null && range.hasUpperBound() ?
            range.upperBoundType() == BoundType.CLOSED : null;
      default:
        throw new IllegalArgumentException("Bad field index");
    }
  }

  @Override
  public Schema getSchema() {
    Schema nullableItem;
    if (itemSchema.getType() == Schema.Type.UNION) {
      Set<Schema> types = Sets.newHashSet(itemSchema.getTypes());
      types.add(Schema.create(Schema.Type.NULL));
      nullableItem = Schema.createUnion(Lists.newArrayList(types));
    } else {
      nullableItem = SchemaBuilder.nullable().type(itemSchema);
    }

    return SchemaBuilder.record(getClass().getName()).fields()
        .name("lowerBound").type(nullableItem).withDefault(null)
        .optionalBoolean("isLowerInclusive")
        .name("upperBound").type(nullableItem).withDefault(null)
        .optionalBoolean("isUpperInclusive")
        .endRecord();
  }
}
