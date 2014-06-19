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

import org.apache.crunch.Pair;
import org.kitesdk.scripting.Script;
import org.kitesdk.scripting.Carrier;
import org.kitesdk.scripting.utils.WrappedEmitter;

@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value="SE_NO_SERIALVERSIONID",
    justification="Purposely not compatible with other versions")
public class FromTable<KI, VI, T> extends Stage<Pair<KI, VI>, T> {
  public FromTable(String name, Script script, Carrier<T> carrier) {
    super(name, script, carrier);
  }

  @Override
  public void process(Pair<KI, VI> input, WrappedEmitter<T> emitter) {
    infected.processPair(input, emitter);
  }
}
