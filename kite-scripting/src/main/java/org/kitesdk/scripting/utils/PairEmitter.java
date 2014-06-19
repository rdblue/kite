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

package org.kitesdk.scripting.utils;

import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

public class PairEmitter<T> implements WrappedEmitter<T> {
  Emitter<Pair> wrapped = null;

  @SuppressWarnings("unchecked")
  public void setEmitter(Emitter<T> wrapped) {
    this.wrapped = (Emitter<Pair>) wrapped;
  }

  @Override
  public <I> void emit(I key) {
    wrapped.emit(Pair.of(key, null));
  }

  @Override
  public <K, V> void emit(K key, V value) {
    wrapped.emit(Pair.of(key, value));
  }
}
