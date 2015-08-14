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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.NullNode;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.impl.Accessor;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.spi.SchemaUtil;
import org.kitesdk.data.spi.partition.IdentityFieldPartitioner;
import org.kududb.ColumnSchema;
import org.kududb.Type;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public class KuduSchemaConverter {
  private static final ImmutableMap<Type, Schema.Type> KUDU_TO_AVRO_TYPE = ImmutableMap
      .<Type, Schema.Type>builder().put(Type.BINARY, Schema.Type.BYTES)
      .put(Type.BOOL, Schema.Type.BOOLEAN)
      .put(Type.DOUBLE, Schema.Type.DOUBLE).put(Type.FLOAT, Schema.Type.FLOAT)
          // TODO: Is this safe?
      .put(Type.INT16, Schema.Type.INT).put(Type.INT32, Schema.Type.INT).put(
          Type.INT64, Schema.Type.LONG)
          // TODO: Is this safe?
      .put(Type.INT8, Schema.Type.INT).put(Type.STRING, Schema.Type.STRING)
          // TODO: Wait for AVRO-739 in 1.8.0
          //.put(Type.TIMESTAMP, Schema.Type.TIMESTAMP)
      .build();

  private static final Schema NULL = Schema.create(Schema.Type.NULL);
  private static final NullNode NULL_DEFAULT = NullNode.getInstance();

  public static Schema schemaFor(String table, Collection<ColumnSchema> columns) {
    List<Schema.Field> fields = Lists.newArrayList();

    for (ColumnSchema column : columns) {
      fields.add(convertField(column.getName(), column));
    }

    Schema recordSchema = Schema.createRecord(
        table, "Schema for Kudu table: " + table, null, false);
    recordSchema.setFields(fields);

    return recordSchema;
  }

  public static PartitionStrategy strategyFor(Collection<ColumnSchema> columns) {
    PartitionStrategy.Builder builder = new PartitionStrategy.Builder();

    for (ColumnSchema column : columns) {
      if (column.isKey()) {
        builder.identity(column.getName());
      }
    }

    return builder.build();
  }

  private static Schema.Field convertField(String name, ColumnSchema column) {
    Schema schema = convert(column.getType());

    // TODO: Default values
    //Object defaultValue = column.getDefaultValue();
    JsonNode defaultValue = null;

    if (column.isNullable()) {
      schema = optional(schema);
      defaultValue = NULL_DEFAULT;
    }

    return new Schema.Field(name, schema, doc(column.getType()), defaultValue);
  }

  private static Schema convert(Type type) {
    Preconditions.checkArgument(KUDU_TO_AVRO_TYPE.containsKey(type),
        "Cannot convert unsupported type: %s", type.name());
    Schema.Type avroType = KUDU_TO_AVRO_TYPE.get(type);
    return Schema.create(avroType);
  }

  private static Schema optional(Schema schema) {
    return Schema.createUnion(Lists.newArrayList(NULL, schema));
  }

  private static String doc(Type type) {
    return "Converted from '" + String.valueOf(type) + "'";
  }

  public static org.kududb.Schema convertSchema(Schema avroSchema, PartitionStrategy strategy) {
    Preconditions.checkArgument(avroSchema.getType() == Schema.Type.RECORD,
        "Cannot use non-record schemas with Kudu: " + avroSchema.toString(true));
    List<ColumnSchema> columns = Lists.newArrayList();

    Set<String> alreadyAdded = Sets.newHashSet();
    List<FieldPartitioner> fps = Accessor.getDefault()
        .getFieldPartitioners(strategy);

    // the partition strategy fields go first, using the schema names.
    // only identity partitioners are allowed.
    for (FieldPartitioner fp : fps) {
      Preconditions.checkArgument(fp instanceof IdentityFieldPartitioner,
          "Unsupported partitioner (only identity is supported): " + fp);
      String source = fp.getSourceName();
      columns.add(convertField(avroSchema.getField(source), true));
      alreadyAdded.add(source);
    }

    // add any fields that were not already added
    for (Schema.Field field : avroSchema.getFields()) {
      if (!alreadyAdded.contains(field.name())) {
        columns.add(convertField(field, false));
      }
    }

    return new org.kududb.Schema(columns);
  }

  private static ColumnSchema convertField(Schema.Field field, boolean isKey) {
    switch (field.schema().getType()) {
      case UNION:
        return new ColumnSchema.ColumnSchemaBuilder(field.name(),
            convert(nonNull(field.schema().getTypes()).getType()))
            .nullable(SchemaUtil.nullOk(field.schema()))
            .key(isKey)
            .build();
      default:
        return new ColumnSchema.ColumnSchemaBuilder(field.name(),
            convert(field.schema().getType()))
            .nullable(SchemaUtil.nullOk(field.schema()))
            .key(isKey)
            .build();
    }
  }

  private static Type convert(Schema.Type type) {
    switch (type) {
      case BYTES:
        return Type.BINARY;
      case BOOLEAN:
        return Type.BOOL;
      case DOUBLE:
        return Type.DOUBLE;
      case FLOAT:
        return Type.FLOAT;
      case INT:
        return Type.INT32;
      case STRING:
        return Type.STRING;
    }
    throw new IllegalArgumentException(
        "Could not convert Avro type " + type.name() + " to Kudu type");
  }

  private static Schema nonNull(List<Schema> schemas) {
    Preconditions.checkArgument(schemas.size() == 2,
        "Cannot get non-null schema from " + schemas);
    if (schemas.get(0).getType() == Schema.Type.NULL) {
      return schemas.get(1);
    } else if (schemas.get(1).getType() == Schema.Type.NULL) {
      return schemas.get(0);
    } else {
      throw new IllegalArgumentException("No null schema: " + schemas);
    }
  }
}
