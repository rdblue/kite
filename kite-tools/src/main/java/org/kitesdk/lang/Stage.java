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


import com.google.common.base.Preconditions;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public interface Stage<S, T> {

  public void setContext(TaskInputOutputContext<?, ?, ?, ?> context);

  public <I> void processSingle(I single, Emitter<T> emitter);

  public <K, V> void processPair(Pair<K, V> pair, Emitter<T> emitter);

  public void initialize();

  public void cleanup(Emitter<T> emitter);

  public int arity();

  abstract class AbstractStage<S, T> implements Stage<S, T> {
    private TaskInputOutputContext<?, ?, ?, ?> context = null;
    private Emitter<T> emitter = null;

    public abstract <I> void process(I input);
    public abstract <K, V> void process(K key, V value);
    public abstract void before();
    public abstract void after();
    public abstract String defaultGroup();

    @Override
    public final void setContext(TaskInputOutputContext<?, ?, ?, ?> context) {
      this.context = context;
    }

    @Override
    public final void initialize() {
      Preconditions.checkNotNull(context, "[BUG] Context is null");
      before();
    }

    @Override
    public final void cleanup(Emitter<T> emitter) {
      this.emitter = emitter;
      after();
      this.emitter = null;
    }

    @SuppressWarnings("unchecked")
    public final void process(S input, Emitter<T> emitter) {
      if (input instanceof Pair) {
        processPair((Pair) input, emitter);
      } else {
        processSingle(input, emitter);
      }
    }

    @Override
    public final <I> void processSingle(I input, Emitter<T> emitter) {
      this.emitter = emitter;
      process(input);
    }

    @Override
    public final <K, V> void processPair(Pair<K, V> pair, Emitter<T> emitter) {
      this.emitter = emitter;
      process(pair.first(), pair.second());
    }

    @SuppressWarnings("unchecked")
    public <K, V> void emit(K key, V value) {
      Preconditions.checkArgument(emitter != null,
          "Cannot call emit outside of processing");
      emitter.emit((T) Pair.of(key, value));
    }

    public void emit(T value) {
      Preconditions.checkArgument(emitter != null,
          "Cannot call emit outside of processing");
      emitter.emit(value);
    }

    public final void increment(Object counter) {
      increment(counter, 1);
    }

    public final void increment(Object counter, long amount) {
      increment(defaultGroup(), counter.toString(), amount);
    }

    public final void increment(Object group, Object counter) {
      increment(group, counter, 1);
    }

    public final void increment(Object group, Object counter, long amount) {
      context.getCounter(group.toString(), counter.toString()).increment(amount);
    }
  }

  public abstract class Arity1<S, T> extends AbstractStage<S, T> {
    public abstract <I> Object call(I one);

    @Override
    public void before() {}

    @Override
    public void after() {}

    @Override
    public String defaultGroup() {
      // TODO: set this correctly
      return "test";
    }

    @Override
    public final <I> void process(I input) {
      call(input);
    }

    @Override
    public <K, V> void process(K key, V value) {
      call(new Object[] {key, value});
    }

    @Override
    public final int arity() {
      return 1;
    }
  }

  public abstract class Arity2<S, T> extends AbstractStage<S, T> {
    public abstract <K, V> Object call(K one, V two);

    @Override
    public void before() {}

    @Override
    public void after() {}

    @Override
    public String defaultGroup() {
      // TODO: set this correctly
      return "test";
    }

    @Override
    public final <I> void process(I input) {
      call(input, null);
    }

    @Override
    public final <K, V> void process(K key, V value) {
      call(key, value);
    }

    @Override
    public final int arity() {
      return 2;
    }
  }
}
