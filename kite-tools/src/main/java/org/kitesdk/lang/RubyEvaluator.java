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

package org.kitesdk.lang;

import com.google.common.base.Charsets;
import java.nio.CharBuffer;
import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import org.apache.commons.io.input.CharSequenceReader;
import org.apache.crunch.types.avro.AvroMode;
import org.jruby.Ruby;
import org.kitesdk.lang.generics.ruby.RubyReaderWriterFactory;

public class RubyEvaluator implements Evaluator {
  static {
    // ensure Ruby is loaded along with this Evaluator
    Ruby.class.isPrimitive();
  }

  @Override
  public void eval(Script script) throws ScriptException {
    CharBuffer cb = Charsets.UTF_8.decode(script.toByteBuffer());

    // use the same runtime and variable map for all ScriptingContainers
    System.setProperty("org.jruby.embed.localcontext.scope", "singleton");
    System.setProperty("org.jruby.embed.compat.version", "JRuby1.9");
    // keep local variables around between calls to eval
    System.setProperty("org.jruby.embed.localvariable.behavior", "persistent");
    // make sure object hashing is consistent across all JVM instances, PR #640
    System.setProperty("jruby.consistent.hashing", "true");

    // use ruby custom avro data
    AvroMode.GENERIC.override(new RubyReaderWriterFactory());

    ScriptEngine engine = new ScriptEngineManager().getEngineByName("jruby");
    Bindings bindings = new SimpleBindings();
    bindings.put("$SCRIPT", script);
    engine.eval(new CharSequenceReader(cb), bindings);
  }

  @Override
  public String ext() {
    return "rb";
  }
}
