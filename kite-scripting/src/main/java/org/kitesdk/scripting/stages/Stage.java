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

package org.kitesdk.scripting.stages;

import java.io.ObjectStreamException;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.types.PType;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.kitesdk.scripting.Script;
import org.kitesdk.scripting.Carrier;
import org.kitesdk.scripting.utils.PairEmitter;
import org.kitesdk.scripting.utils.SingleEmitter;
import org.kitesdk.scripting.utils.WrappedEmitter;

public abstract class Stage<S, T> extends DoFn<S, T> {

  private final String name;
  private final Script script;
  protected transient Carrier<T> infected = null;
  protected transient boolean emitPairs = false;
  protected transient WrappedEmitter<T> wrapper = null;

  public Stage(String name, Script script, Carrier<T> infected) {
    this.name = name;
    this.script = script;
    this.infected = infected;
  }

  public String name() {
    return infected.name();
  }

  @Override
  public void setContext(TaskInputOutputContext<?, ?, ?, ?> context) {
    super.setContext(context);
    infected.setContext(context);
  }

  @Override
  public void initialize() {
    super.initialize();
    infected.initialize();
    this.wrapper = emitPairs ? new PairEmitter<T>() : new SingleEmitter<T>();
  }

  @Override
  public void cleanup(Emitter<T> emitter) {
    wrapper.setEmitter(emitter);
    infected.cleanup(wrapper);
    super.cleanup(emitter);
  }

  public void emitPairs() {
    this.emitPairs = true;
  }

  @SuppressWarnings("unchecked")
  public PType<T> resultType() {
    PType<?> keyType = infected.keyType();
    if (emitPairs) {
      return (PType<T>) keyType.getFamily().tableOf(keyType, infected.valueType());
    } else {
      return (PType<T>) keyType;
    }
  }

  @Override
  public final void process(S input, Emitter<T> emitter) {
    wrapper.setEmitter(emitter);
    process(input, wrapper);
  }

  public abstract void process(S input, WrappedEmitter<T> emitter);

  protected Object writeReplace() throws ObjectStreamException {
    return new StandIn(name, script);
  }

}
