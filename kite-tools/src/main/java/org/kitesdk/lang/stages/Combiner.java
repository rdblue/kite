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

package org.kitesdk.lang.stages;

import java.io.ObjectStreamException;
import org.apache.crunch.CombineFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.kitesdk.lang.Carrier;
import org.kitesdk.lang.Script;
import org.kitesdk.lang.utils.PairEmitter;
import org.kitesdk.lang.utils.WrappedEmitter;

@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value="SE_NO_SERIALVERSIONID",
    justification="Purposely not compatible with other versions")
public class Combiner<K, V> extends CombineFn<K, V> {

  private final String name;
  private final Script script;
  protected transient Carrier<Pair<K, V>> carrier = null;
  protected transient WrappedEmitter<Pair<K, V>> wrapper = null;

  public Combiner(String name, Script script,
                    Carrier<Pair<K, V>> carrier) {
    this.name = name;
    this.script = script;
    this.carrier = carrier;
  }

  @Override
  public void setContext(TaskInputOutputContext<?, ?, ?, ?> context) {
    super.setContext(context);
    carrier.setContext(context);
  }

  @Override
  public void initialize() {
    super.initialize();
    carrier.initialize();
    this.wrapper = new PairEmitter<Pair<K, V>>();
  }

  @Override
  public void cleanup(Emitter<Pair<K, V>> emitter) {
    wrapper.setEmitter(emitter);
    carrier.cleanup(wrapper);
    super.cleanup(emitter);
  }

  @SuppressWarnings("unchecked")
  public PTableType<K, V> resultType() {
    PType<K> keyType = (PType<K>) carrier.keyType();
    return keyType.getFamily().tableOf(keyType, (PType<V>) carrier.valueType());
  }

  @Override
  public void process(Pair<K, Iterable<V>> input, Emitter<Pair<K, V>> emitter) {
    wrapper.setEmitter(emitter);
    carrier.processPair(input, wrapper);
  }

  protected Object writeReplace() throws ObjectStreamException {
    return new StandIn(name, script);
  }
}
