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

package org.kitesdk.data.kudu.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.avro.Schema;
import org.codehaus.jackson.node.NullNode;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.impl.Accessor;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.spi.SchemaUtil;
import org.kududb.ColumnSchema;
import org.kududb.Type;
import org.w3c.dom.TypeInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class KuduSchemaConverter {
  private static final ImmutableMap<Type, Schema.Type> KUDU_TO_AVRO_TYPE =
      ImmutableMap.<Type, Schema.Type>builder()
      .put(Type.BINARY, Schema.Type.BYTES)
          .put(Type.BOOL, Schema.Type.BOOLEAN)
          .put(Type.DOUBLE, Schema.Type.DOUBLE)
          .put(Type.FLOAT, Schema.Type.FLOAT)
          // TODO: Is this safe?
          .put(Type.INT16, Schema.Type.INT)
          .put(Type.INT32, Schema.Type.INT)
          .put(Type.INT64, Schema.Type.LONG)
          // TODO: Is this safe?
          .put(Type.INT8, Schema.Type.INT)
          .put(Type.STRING, Schema.Type.STRING)
          // TODO: Wait for AVRO-739 in 1.8.0
          //.put(Type.TIMESTAMP, Schema.Type.TIMESTAMP)
          .build();

  private static final Schema NULL = Schema.create(Schema.Type.NULL);
  @VisibleForTesting
  static final NullNode NULL_DEFAULT = NullNode.getInstance();
  @VisibleForTesting
  static final Collection<String[]> NO_REQUIRED_FIELDS = ImmutableList.of();

  public static Schema convertTable(String table, Collection<ColumnSchema> columns,
                                    @Nullable PartitionStrategy strategy) {
    ArrayList<String> fieldNames = Lists.newArrayList();
    ArrayList<Type> fieldTypes = Lists.newArrayList();
    LinkedList<String> start = Lists.newLinkedList();
    Collection<String[]> requiredFields = requiredFields(strategy);

    List<Schema.Field> fields = Lists.newArrayList();
    for (ColumnSchema column : columns) {
      // pass null for the initial path to exclude the table name
      Type type = column.getType();
      fieldNames.add(column.getName());
      fieldTypes.add(type);
      fields.add(convertField(start, column.getName(), type, requiredFields));
    }

    /*StructTypeInfo struct = new StructTypeInfo();
    struct.setAllStructFieldNames(fieldNames);
    struct.setAllStructFieldTypeInfos(fieldTypes);

    Schema recordSchema = Schema.createRecord(table, doc(struct), null, false);
    recordSchema.setFields(fields);

    recordSchema.getFields().get(0).schema().getType()*/
    return null;
  }

  private static Schema.Field convertField(LinkedList<String> path, String name,
                                           Type type,
                                           Collection<String[]> required) {
    // filter the required fields with the current name
    Collection<String[]> matchingRequired = filterByStartsWith(required, path, name);

    Schema schema = convert(path, name, type, matchingRequired);

    return new Schema.Field(name, schema, doc(type),
         null);
  }

  @VisibleForTesting
  static Schema convert(LinkedList<String> path, String name,
                        Type type, Collection<String[]> required) {
    Preconditions.checkArgument(KUDU_TO_AVRO_TYPE.containsKey(type),
        "Cannot convert unsupported type: %s", type.name());
    Schema.Type avroType = KUDU_TO_AVRO_TYPE.get(type);
      return Schema.create(avroType);
  }

  @VisibleForTesting
  static Schema optional(Schema schema) {
    return Schema.createUnion(Lists.newArrayList(NULL, schema));
  }

  private static String doc(Type type) {
    return "Converted from '" + String.valueOf(type) + "'";
  }

  public static org.kududb.Schema convertSchema(Schema avroSchema) {
    /*List<FieldSchema> columns = Lists.newArrayList();
    if (Schema.Type.RECORD.equals(avroSchema.getType())) {
      for (Schema.Field field : avroSchema.getFields()) {
        columns.add(new FieldSchema(
            field.name(), convert(field.schema()).getTypeName(), field.doc()));
      }
    } else {
      columns.add(new FieldSchema(
          "column", convert(avroSchema).getTypeName(), avroSchema.getDoc()));
    }
    return columns;
    */
    return null;
  }
  
  private static Collection<String[]> filterByStartsWith(
      Collection<String[]> fields, LinkedList<String> path, String name) {
    path.addLast(name);

    List<String[]> startsWithCollection = Lists.newArrayList();
    for (String[] field : fields) {
      if (startsWith(field, path)) {
        startsWithCollection.add(field);
      }
    }

    path.removeLast();

    return startsWithCollection;
  }

  /**
   * Returns true if left starts with right.
   */
  private static boolean startsWith(String[] left, List<String> right) {
    // short circuit if a match isn't possible
    if (left.length < right.size()) {
      return false;
    }

    for (int i = 0; i < right.size(); i += 1) {
      if (!left[i].equals(right.get(i))) {
        return false;
      }
    }

    return true;
  }

  @SuppressWarnings("deprecation")
  private static Collection<String[]> requiredFields(@Nullable PartitionStrategy strategy) {
    if (strategy == null) {
      return NO_REQUIRED_FIELDS;
    }

    List<String[]> requiredFields = Lists.newArrayList();
    for (FieldPartitioner fp : Accessor.getDefault().getFieldPartitioners(strategy)) {
      // source name is not present for provided partitioners
      if (fp.getSourceName() != null) {
        requiredFields.add(fp.getSourceName().split("\\."));
      }
    }

    return requiredFields;
  }
}
