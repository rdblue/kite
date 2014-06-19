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

package org.kitesdk.scripting.generics.python;

import com.google.common.base.Throwables;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;
import org.kitesdk.scripting.generics.DataFactory;
import org.python.core.PyDictionary;
import org.python.core.PyList;
import org.python.core.PyString;
import org.python.core.PyStringMap;

public class PyDataFactory implements DataFactory
    <PyStringMap, PyDictionary, PyList, PyString, PyString> {

  private static final Charset UTF8 = Charset.forName("utf-8");
  private static final ThreadLocal<CharsetDecoder> DECODERS =
      new ThreadLocal<CharsetDecoder>() {
        @Override
        protected CharsetDecoder initialValue() {
          return UTF8.newDecoder();
        }
      };

  private static final PyDataFactory INSTANCE = new PyDataFactory();

  public static PyDataFactory get() {
    return INSTANCE;
  }

  private PyDataFactory() {
  }

  @Override
  public PyStringMap createRecord(String name, List<String> fieldNames) {
    // Python's equivalent of RubyStruct, namedtuple, is just a method that
    // evals a String to create the right class :(
    return new PyStringMap();
  }

  @Override
  public PyDictionary createMap() {
    return new PyDictionary();
  }

  @Override
  public PyList createList() {
    return new PyList();
  }

  @Override
  public PyList createList(int capacity) {
    return createList();
  }

  @Override
  public PyString createString(String javaString) {
    return new PyString(javaString);
  }

  @Override
  public PyString createString(ByteBuffer utf8) {
    // PyStrings are backed String, utf-16, so translate to String and wrap
    CharsetDecoder decoder = DECODERS.get();
    try {
      return createString(decoder.decode(utf8).toString());
    } catch (CharacterCodingException ex) {
      // Can't decode utf-8 ?!
      throw Throwables.propagate(ex);
    }
  }

  @Override
  public PyString createSymbol(String symbol) {
    // ensure the PyString is interned
    PyString sym = createString(symbol);
    sym.internedString();
    return sym;
  }
}
