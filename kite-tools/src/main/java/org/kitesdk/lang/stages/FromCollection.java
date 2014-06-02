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

import org.kitesdk.lang.Carrier;
import org.kitesdk.lang.Script;
import org.kitesdk.lang.utils.WrappedEmitter;

@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value="SE_NO_SERIALVERSIONID",
    justification="Purposely not compatible with other versions")
public class FromCollection<S, T> extends Stage<S, T> {
  public FromCollection(String name, Script script, Carrier<T> carrier) {
    super(name, script, carrier);
  }

  @Override
  public void process(S input, WrappedEmitter<T> emitter) {
    infected.processSingle(input, emitter);
  }
}
