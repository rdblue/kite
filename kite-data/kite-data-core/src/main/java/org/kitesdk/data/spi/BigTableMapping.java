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

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;
import org.kitesdk.data.RecordMapping;

public class BigTableMapping extends RecordMapping {

  private final Collection<FieldMapping> fieldMappings;

  public BigTableMapping(Collection<FieldMapping> mappings) {
    fieldMappings = ImmutableList.copyOf(mappings);
  }

  public Collection<FieldMapping> getFieldMappings() {
    return fieldMappings;
  }

  public FieldMapping getFieldMapping(String fieldName) {
    for (FieldMapping fm : fieldMappings) {
      if (fm.getFieldName().equals(fieldName)) {
        return fm;
      }
    }
    return null;
  }

  /**
   * Get the columns required by this schema.
   *
   * @return The set of columns
   */
  public Set<String> getRequiredColumns() {
    Set<String> set = new HashSet<String>();
    for (FieldMapping fieldMapping : fieldMappings) {
      if (FieldMapping.MappingType.KEY != fieldMapping.getMappingType()) {
        set.add(fieldMapping.getFamilyAsString() + ":"
            + fieldMapping.getQualifierAsString());
      }
    }
    return set;
  }

  /**
   * Get the column families required by this schema.
   *
   * @return The set of column families.
   */
  public Set<String> getRequiredColumnFamilies() {
    Set<String> set = new HashSet<String>();
    for (FieldMapping mapping : fieldMappings) {
      if (FieldMapping.MappingType.KEY != mapping.getMappingType())
        set.add(mapping.getFamilyAsString());
    }
    return set;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BigTableMapping that = (BigTableMapping) o;
    return Objects.equal(fieldMappings, that.fieldMappings);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(fieldMappings.hashCode());
  }

  @Override
  public String toString() {
    return RecordMappingParser.toString(this, false);
  }

  public String toString(boolean pretty) {
    return RecordMappingParser.toString(this, true);
  }

  /**
   * A fluent builder to aid in constructing a {@link org.kitesdk.data.RecordMapping}.
   */
  public static class Builder extends RecordMapping.Builder {

    /**
     * Adds a {@link FieldMapping}.
     *
     * @param fieldMapping A {@code FieldMapping}
     * @return This Builder for method chaining
     */
    public Builder fieldMapping(FieldMapping fieldMapping) {
      addField(fieldMapping);
      return this;
    }

    /**
     * Adds each {@link FieldMapping} from a collection.
     *
     * @param fieldMappings A collection of {@code FieldMapping} objects
     * @return This Builder for method chaining
     */
    public Builder fieldMappings(Collection<FieldMapping> fieldMappings) {
      for (FieldMapping fieldMapping : fieldMappings) {
        addField(fieldMapping);
      }
      return this;
    }

  }
}
