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

package org.kitesdk.lang.carriers;

import java.io.ObjectStreamException;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.kitesdk.lang.Script;
import org.kitesdk.lang.Stage;

public abstract class Carrier<S, T> extends DoFn<S, T> {

  private final String name;
  private final Script script;
  protected transient final Stage<S, T> stage;

  public Carrier(String name, Script script, Stage<S, T> stage) {
    this.name = name;
    this.script = script;
    this.stage = stage;
  }

  @Override
  public void setContext(TaskInputOutputContext<?, ?, ?, ?> context) {
    super.setContext(context);
    stage.setContext(context);
  }

  @Override
  public void initialize() {
    super.initialize();
    stage.initialize();
  }

  @Override
  public void cleanup(Emitter<T> emitter) {
    stage.cleanup(emitter);
    super.cleanup(emitter);
  }

  public abstract void process(S input, Emitter<T> emitter);

  protected Object writeReplace() throws ObjectStreamException {
    return new StandIn(name, script);
  }

}
