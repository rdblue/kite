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

package org.kitesdk.scripting;

import com.google.common.base.Charsets;
import java.nio.CharBuffer;
import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import org.apache.commons.io.input.CharSequenceReader;
import org.apache.crunch.types.avro.AvroMode;
import org.kitesdk.scripting.generics.python.PyReaderWriterFactory;
import org.python.core.Py;
import org.python.core.PyObject;
import org.python.core.PySystemState;

public class PyEvaluator implements Evaluator {
  static {
    // ensure Python is loaded along with this Evaluator
    PySystemState.class.isPrimitive();
  }

  @Override
  public void eval(Script script) throws ScriptException {
    CharBuffer cb = Charsets.UTF_8.decode(script.toByteBuffer());

    // a hack for python to pass the analytic back
    PySystemState engineSys = new PySystemState();
    PyObject builtins = engineSys.getBuiltins();
    builtins.__setitem__("_script", Py.java2py(script));
    Py.setSystemState(engineSys);

    // use ruby custom avro data
    AvroMode.GENERIC.override(new PyReaderWriterFactory());

    ScriptEngine engine = new ScriptEngineManager().getEngineByName("python");
    Bindings bindings = new SimpleBindings();
    engine.eval(new CharSequenceReader(cb), bindings);
  }

  @Override
  public String ext() {
    return "py";
  }
}
