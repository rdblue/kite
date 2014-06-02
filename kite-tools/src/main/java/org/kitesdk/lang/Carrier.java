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
import org.apache.crunch.Pair;
import org.apache.crunch.types.PType;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.kitesdk.lang.utils.WrappedEmitter;

public interface Carrier<T> {

  public void setContext(TaskInputOutputContext<?, ?, ?, ?> context);

  public <I> void processSingle(I single, WrappedEmitter<T> emitter);

  public <I1, I2> void processPair(Pair<I1, I2> pair, WrappedEmitter<T> emitter);

  public void initialize();

  public void cleanup(WrappedEmitter<T> emitter);

  public String name();

  public Type type();

  public PType<?> keyType();

  public PType<?> valueType();

  public int arity();

  public enum Type {
    PARALLEL, COMBINE, REDUCE
  }

  abstract static class AbstractCarrier<T> implements Carrier<T> {
    private TaskInputOutputContext<?, ?, ?, ?> context = null;
    private WrappedEmitter<T> emitter = null;

    public abstract <I> void process(I input);
    public abstract <I1, I2> void process(I1 key, I2 value);
    public abstract void before();
    public abstract void after();

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
    public final void cleanup(WrappedEmitter<T> emitter) {
      this.emitter = emitter;
      after();
      this.emitter = null;
    }

    @Override
    public final <I> void processSingle(I input, WrappedEmitter<T> emitter) {
      this.emitter = emitter;
      process(input);
    }

    @Override
    public final <I1, I2> void processPair(Pair<I1, I2> pair, WrappedEmitter<T> emitter) {
      this.emitter = emitter;
      process(pair.first(), pair.second());
    }

    public <K, V> void emit(K key, V value) {
      Preconditions.checkArgument(emitter != null,
          "Cannot call emit outside of processing");
      emitter.emit(key, value);
    }

    public <I> void emit(I value) {
      Preconditions.checkArgument(emitter != null,
          "Cannot call emit outside of processing");
      emitter.emit(value);
    }

    public final void increment(Object counter) {
      increment(counter, 1);
    }

    public final void increment(Object counter, long amount) {
      increment(name(), counter.toString(), amount);
    }

    public final void increment(Object group, Object counter) {
      increment(group, counter, 1);
    }

    public final void increment(Object group, Object counter, long amount) {
      context.getCounter(group.toString(), counter.toString()).increment(amount);
    }
  }

  public abstract static class Arity1<T> extends AbstractCarrier<T> {
    public abstract <I> Object call(I one);

    @Override
    public void before() {}

    @Override
    public void after() {}

    @Override
    public final <I> void process(I input) {
      call(input);
    }

    @Override
    public final <I1, I2> void process(I1 key, I2 value) {
      // TODO: What should be passed: key, value, or both?
      call(new Object[] {key, value});
    }

    @Override
    public final int arity() {
      return 1;
    }
  }

  public abstract static class Arity2<T> extends AbstractCarrier<T> {
    public abstract <I1, I2> Object call(I1 one, I2 two);

    @Override
    public void before() {}

    @Override
    public void after() {}

    @Override
    public final <I> void process(I input) {
      call(input, null);
    }

    @Override
    public final <I1, I2> void process(I1 key, I2 value) {
      call(key, value);
    }

    @Override
    public final int arity() {
      return 2;
    }
  }
}
