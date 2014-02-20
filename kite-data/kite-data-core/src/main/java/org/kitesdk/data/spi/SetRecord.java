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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Collection;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;

public class SetRecord implements IndexedRecord {
  private static final Function<Object, String> TO_STRING =
      new Function<Object, String>() {
    @Override
    public String apply(@Nullable Object input) {
      return input == null ? null : input.toString();
    }
  };

  private final Schema itemSchema;
  private Predicate predicate;

  public SetRecord(Schema itemSchema) {
    this.itemSchema = itemSchema;
  }

  public void setPredicate(Predicate predicate) {
    this.predicate = predicate;
  }

  public Predicate getPredicate() {
    return predicate;
  }
  @Override
  @SuppressWarnings("unchecked")
  public void put(int i, Object v) {
    Preconditions.checkArgument(i == 0, "Bad field index");
    Collection<Object> items = (Collection<Object>) v;
    if (items.isEmpty()) {
      this.predicate = Predicates.exists();
    } else {
      if (items.iterator().next() instanceof Utf8) {
        this.predicate = Predicates.in(Sets.newHashSet(
            Iterables.transform(items, TO_STRING)));
      } else {
        this.predicate = Predicates.in(Sets.newHashSet(items));
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object get(int i) {
    Preconditions.checkArgument(i == 0, "Bad field index");
    if (predicate instanceof Predicates.In) {
      return Lists.newArrayList(((Predicates.In) predicate).getSet());
    } else {
      return Lists.newArrayList();
    }
  }

  @Override
  public Schema getSchema() {
    return SchemaBuilder.record(getClass().getName()).fields()
        .name("items").type(Schema.createArray(itemSchema)).noDefault()
        .endRecord();
  }
}
