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
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;
import com.google.common.collect.Sets;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.junit.Assert;
import org.junit.Test;

public class TestConstraintSerialization {
  @Test
  public void testRange() throws IOException {
    Schema itemSchema = SchemaBuilder.builder().intType();
    RangeRecord rec = new RangeRecord(itemSchema);

    Range range = Ranges.closed(34, 43);
    rec.setRange(range);

    RangeRecord rtRec = roundTrip(rec);
    Assert.assertTrue(rtRec == rec);               // RangeRecord is reused
    Assert.assertEquals(range, rtRec.getRange());  // Range produced is equal
    Assert.assertFalse(range == rtRec.getRange()); // Range is not identical
  }

  @Test
  public void testRangeWithUnion() throws IOException {
    // making the itemSchema nullable even when nullable works
    Schema itemSchema = SchemaBuilder.builder()
        .unionOf().intType().and().stringType().and().nullType().endUnion();
    RangeRecord rec = new RangeRecord(itemSchema);

    Range range = Ranges.closed(34, 43);
    rec.setRange(range);

    RangeRecord rtRec = roundTrip(rec);
    Assert.assertTrue(rtRec == rec);               // RangeRecord is reused
    Assert.assertEquals(range, rtRec.getRange());  // Range produced is equal
    Assert.assertFalse(range == rtRec.getRange()); // Range is not identical

    range = Ranges.closed("a", "b");
    rec.setRange(range);

    rtRec = roundTrip(rec);
    Assert.assertTrue(rtRec == rec);               // RangeRecord is reused
    Assert.assertEquals(range, rtRec.getRange());  // Range produced is equal
    Assert.assertFalse(range == rtRec.getRange()); // Range is not identical
  }

  @Test
  public void testRangeBoundTypes() throws IOException {
    Schema itemSchema = SchemaBuilder.builder().intType();
    RangeRecord rec = new RangeRecord(itemSchema);

    Range range = Ranges.closed(34, 43);
    rec.setRange(range);

    RangeRecord rtRec = roundTrip(rec);
    Assert.assertTrue(rtRec == rec);               // RangeRecord is reused
    Assert.assertEquals(range, rtRec.getRange());  // Range produced is equal
    Assert.assertFalse(range == rtRec.getRange()); // Range is not identical

    range = Ranges.closedOpen(34, 43);
    rec.setRange(range);

    rtRec = roundTrip(rec);
    Assert.assertTrue(rtRec == rec);               // RangeRecord is reused
    Assert.assertEquals(range, rtRec.getRange());  // Range produced is equal
    Assert.assertFalse(range == rtRec.getRange()); // Range is not identical

    range = Ranges.openClosed(34, 43);
    rec.setRange(range);

    rtRec = roundTrip(rec);
    Assert.assertTrue(rtRec == rec);               // RangeRecord is reused
    Assert.assertEquals(range, rtRec.getRange());  // Range produced is equal
    Assert.assertFalse(range == rtRec.getRange()); // Range is not identical

    range = Ranges.open(34, 43);
    rec.setRange(range);

    rtRec = roundTrip(rec);
    Assert.assertTrue(rtRec == rec);               // RangeRecord is reused
    Assert.assertEquals(range, rtRec.getRange());  // Range produced is equal
    Assert.assertFalse(range == rtRec.getRange()); // Range is not identical
  }

  @Test
  public void testExists() throws IOException {
    Schema itemSchema = SchemaBuilder.builder().intType();
    SetRecord rec = new SetRecord(itemSchema);

    Predicate exists = Predicates.exists();
    rec.setPredicate(exists);

    SetRecord rtRec = roundTrip(rec);
    Assert.assertTrue(rtRec == rec);                   // SetRecord is reused
    Assert.assertTrue(exists == rtRec.getPredicate()); // Exists is a singleton
  }

  @Test
  public void testIn() throws IOException {
    Schema itemSchema = SchemaBuilder.builder().intType();
    SetRecord rec = new SetRecord(itemSchema);

    Predicate in = Predicates.in(Sets.newHashSet(7, 14, 21, 28, 35, 42, 49));
    rec.setPredicate(in);

    SetRecord rtRec = roundTrip(rec);
    Assert.assertTrue(rtRec == rec);                // SetRecord is reused
    Assert.assertEquals(in, rtRec.getPredicate());  // Predicate is equal
    Assert.assertFalse(in == rtRec.getPredicate()); // Not identical
  }

  @Test
  public void testInWithUnion() throws IOException {
    Schema itemSchema = SchemaBuilder.builder()
        .unionOf().intType().and().stringType().endUnion();
    SetRecord rec = new SetRecord(itemSchema);

    Predicate in = Predicates.in(Sets.newHashSet(7, 14, 21, 28, 35, 42, 49));
    rec.setPredicate(in);

    SetRecord rtRec = roundTrip(rec);
    Assert.assertTrue(rtRec == rec);                // SetRecord is reused
    Assert.assertEquals(in, rtRec.getPredicate());  // Predicate is equal
    Assert.assertFalse(in == rtRec.getPredicate()); // Not identical

    in = Predicates.in(Sets.newHashSet("x", "y", "z"));
    rec.setPredicate(in);

    rtRec = roundTrip(rec);
    Assert.assertTrue(rtRec == rec);                // SetRecord is reused
    Assert.assertEquals(in, rtRec.getPredicate());  // Predicate is equal
    Assert.assertFalse(in == rtRec.getPredicate()); // Not identical
  }

  @Test
  public void testInAndExists() throws IOException {
    Schema itemSchema = SchemaBuilder.builder()
        .unionOf().intType().and().stringType().endUnion();
    SetRecord rec = new SetRecord(itemSchema);

    Predicate in = Predicates.in(Sets.newHashSet(7, 14, 21, 28, 35, 42, 49));
    rec.setPredicate(in);

    SetRecord rtRec = roundTrip(rec);
    Assert.assertTrue(rtRec == rec);                // SetRecord is reused
    Assert.assertEquals(in, rtRec.getPredicate());  // Predicate is equal
    Assert.assertFalse(in == rtRec.getPredicate()); // Not identical

    Predicate exists = Predicates.exists();
    rec.setPredicate(exists);

    rtRec = roundTrip(rec);
    Assert.assertTrue(rtRec == rec);                   // SetRecord is reused
    Assert.assertTrue(exists == rtRec.getPredicate()); // Exists is a singleton

    in = Predicates.in(Sets.newHashSet("x", "y", "z"));
    rec.setPredicate(in);

    rtRec = roundTrip(rec);
    Assert.assertTrue(rtRec == rec);                // SetRecord is reused
    Assert.assertEquals(in, rtRec.getPredicate());  // Predicate is equal
    Assert.assertFalse(in == rtRec.getPredicate()); // Not identical

    rec.setPredicate(exists);

    rtRec = roundTrip(rec);
    Assert.assertTrue(rtRec == rec);                   // SetRecord is reused
    Assert.assertTrue(exists == rtRec.getPredicate()); // Exists is a singleton
  }

  @Test
  public void testConstraintWithRange() throws IOException {
    Schema itemSchema = SchemaBuilder.builder().intType();

    Range range = Ranges.closed(34, 43);
    ConstraintRecord rec = new ConstraintRecord(itemSchema);
    rec.setConstraint("testRange", range);

    ConstraintRecord rtRec = roundTrip(rec);
    Assert.assertEquals("testRange", rtRec.getSourceName());
    Assert.assertTrue(rtRec == rec);                 // ConstraintRecord reused
    Assert.assertEquals(range, rtRec.getPredicate());  // Predicate is equal
    Assert.assertFalse(range == rtRec.getPredicate()); // Not identical
  }

  @Test
  public void testConstraintWithIn() throws IOException {
    Schema itemSchema = SchemaBuilder.builder().intType();

    Predicate in = Predicates.in(Sets.newHashSet(7, 14, 21, 28, 35, 42, 49));
    ConstraintRecord rec = new ConstraintRecord(itemSchema);
    rec.setConstraint("testIn", in);

    ConstraintRecord rtRec = roundTrip(rec);
    Assert.assertEquals("testIn", rtRec.getSourceName());
    Assert.assertTrue(rtRec == rec);              // ConstraintRecord is reused
    Assert.assertEquals(in, rec.getPredicate());  // Predicate is equal
    Assert.assertFalse(in == rec.getPredicate()); // Not identical
  }

  @Test
  public void testMixedPredicates() throws IOException {
    Schema itemSchema = SchemaBuilder.builder().intType();

    Range range = Ranges.closed(34, 43);
    ConstraintRecord rec = new ConstraintRecord(itemSchema);
    rec.setConstraint("testRange", range);

    ConstraintRecord rtRec = roundTrip(rec);
    Assert.assertEquals("testRange", rtRec.getSourceName());
    Assert.assertTrue(rtRec == rec);                 // ConstraintRecord reused
    Assert.assertEquals(range, rec.getPredicate());  // Predicate is equal
    Assert.assertFalse(range == rec.getPredicate()); // Not identical

    Predicate in = Predicates.in(Sets.newHashSet(7, 14, 21, 28, 35, 42, 49));
    rec.setConstraint("testIn", in);

    rtRec = roundTrip(rec);
    Assert.assertEquals("testIn", rtRec.getSourceName());
    Assert.assertTrue(rtRec == rec);              // ConstraintRecord is reused
    Assert.assertEquals(in, rec.getPredicate());  // Predicate is equal
    Assert.assertFalse(in == rec.getPredicate()); // Not identical

    Predicate exists = Predicates.exists();
    rec.setConstraint("testExists", exists);

    rtRec = roundTrip(rec);
    Assert.assertEquals("testExists", rtRec.getSourceName());
    Assert.assertTrue(rtRec == rec);                   // SetRecord is reused
    Assert.assertTrue(exists == rtRec.getPredicate()); // Exists is a singleton
  }

  @Test
  public void testSerializationMethods() throws IOException {
    Schema recordSchema = SchemaBuilder.builder().record("TestRecord")
        .fields()
        .requiredInt("int")
        .requiredString("str")
        .optionalDouble("double")
        .optionalLong("long")
        .requiredFloat("float")
        .endRecord();
    Map<String, Predicate> expected = Maps.newHashMap();
    expected.put("int", Ranges.closedOpen(34, 43));
    expected.put("str", Predicates.in(Sets.newHashSet("a", "b", "c")));
    expected.put("double", Predicates.exists());
    expected.put("long", Ranges.atMost(34));

    Map<String, Predicate> actual = Constraints.deserialize(
        recordSchema, Constraints.serialize(recordSchema, expected));
    Assert.assertEquals(expected, actual);
  }

  @SuppressWarnings("unchecked")
  public static <T extends IndexedRecord> T roundTrip(T record) throws IOException {
    SpecificData data = SpecificData.get();
    DatumWriter<IndexedRecord> writer =
        data.createDatumWriter(record.getSchema());

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder enc = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(record, enc);
    enc.flush();

    BinaryDecoder dec = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
    DatumReader<IndexedRecord> reader = data.createDatumReader(record.getSchema());

    return (T) reader.read(record, dec);
  }
}
