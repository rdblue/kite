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

import com.google.common.base.Predicate;
import com.google.common.collect.Range;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.IndexedRecord;

public class ConstraintRecord implements IndexedRecord {
  // reused records for reading/writing predicates by type
  private final SetRecord setRecord;
  private final RangeRecord rangeRecord;

  // state for the current predicate
  private String sourceName = null;
  private boolean isRange = false;
  private Predicate predicate = null;

  ConstraintRecord(Schema itemSchema) {
    this.setRecord = new SetRecord(itemSchema);
    this.rangeRecord = new RangeRecord(itemSchema);
  }

  public void setConstraint(String sourceName, Predicate predicate) {
    this.sourceName = sourceName;
    this.predicate = predicate;
  }

  public String getSourceName() {
    return sourceName;
  }

  public Predicate getPredicate() {
    return predicate;
  }

  @Override
  public void put(int i, Object v) {
    switch (i) {
      case 0:
        this.sourceName = v.toString();
        break;
      case 1:
        this.isRange = (Boolean) v;
        this.predicate = null; // clear any previous predicate
        break;
      case 2:
        if (isRange) {
          this.predicate = rangeRecord.getRange();
        } else {
          this.predicate = setRecord.getPredicate();
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
        return sourceName;
      case 1:
        return (predicate instanceof Range);
      case 2:
        // when this is used to write records
        if (predicate != null) {
          if (predicate instanceof Range) {
            rangeRecord.setRange((Range) predicate);
            return rangeRecord;
          } else {
            setRecord.setPredicate(predicate);
            return setRecord;
          }
        }
        // otherwise, reading records and this should return the right one
        return isRange ? rangeRecord : setRecord;
      default:
        throw new IllegalArgumentException("Bad field index");
    }
  }

  @Override
  public Schema getSchema() {
    Schema predicate = SchemaBuilder.unionOf()
        .type(setRecord.getSchema()).and().type(rangeRecord.getSchema())
        .endUnion();
    return SchemaBuilder.record(getClass().getName())
        .fields()
        .requiredString("source_name")
        .requiredBoolean("is_range")
        .name("predicate").type(predicate).noDefault()
        .endRecord();
  }
}
