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
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import javax.script.ScriptException;
import org.apache.crunch.CombineFn;
import org.apache.crunch.DoFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Source;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.kitesdk.lang.carriers.Combiner;
import org.kitesdk.lang.carriers.FromCollection;
import org.kitesdk.lang.carriers.FromGroupedTable;
import org.kitesdk.lang.carriers.FromTable;
import org.kitesdk.lang.utils.ToTableShim;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value="SE_NO_SERIALVERSIONID",
    justification="Purposely not compatible with other versions")
public class Script implements Serializable, Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(Script.class);

  /*
   * This cache is used to avoid evaluating the script multiple times when
   * this is deserialized. There can be multiple copies of this object, but
   * they will all go through this static cache to eval the analytic bytes.
   */
  private static Cache<String, Script> scripts = CacheBuilder.newBuilder().build();

  public static Script get(String name, InputStream in) {
    byte[] bytes;
    try {
      bytes =  ByteStreams.toByteArray(in);
    } catch (IOException e) {
      throw new IllegalArgumentException("Cannot read script content", e);
    }
    return getOrEval(name, bytes);
  }

  private static Script getOrEval(final String name, final byte[] in) {
    try {
      return scripts.get(name, new Callable<Script>() {
        @Override
        public Script call() throws Exception {
          return new Script(name, in);
        }
      });
    } catch (ExecutionException e) {
      throw Throwables.propagate(e.getCause());
    }
  }

  public enum StageType {
    PARALLEL, COMBINE, REDUCE
  }

  public enum CarrierType {
    COMBINER, FROM_TABLE, FROM_GROUPED_TABLE, FROM_COLLECTION
  }

  private String name;
  private byte[] bytes;

  private transient boolean evaled = false;
  private transient Pipeline pipeline = null;
  private transient PCollection<?> lastCollection = null;
  private transient Map<String, Stage> stagesByName = null;
  private transient Map<String, DoFn> fnsByName = null;
  private transient Map<String, PCollection> collectionsByName = null;

  private Script(String name, byte[] bytes) {
    this.name = name;
    this.bytes = bytes;
  }

  public String getName() {
    return name;
  }

  public PCollection<String> read(String collection) {
    // TODO: Add Kite URI support
    PCollection<String> fileContent = getPipeline().readTextFile(collection);
    lastCollection = fileContent;
    return fileContent;
  }

  public <E> PCollection<E> read(Source<E> collection) {
    PCollection<E> content = getPipeline().read(collection);
    lastCollection = content;
    return content;
  }

  public void write(Target target) {
    getPipeline().write(lastCollection, target);
  }

  public void write(Target target, Target.WriteMode mode) {
    getPipeline().write(lastCollection, target, mode);
  }

  public void write(String target) {
    getPipeline().writeTextFile(lastCollection, target);
  }

  public void enableDebug() {
    getPipeline().enableDebug();
  }

  public ByteBuffer toByteBuffer() {
    // because the buffer itself can have state, create a new one each time
    return ByteBuffer.wrap(bytes).asReadOnlyBuffer();
  }

  public void setPipeline(Pipeline pipeline) {
    this.pipeline = pipeline;
  }

  public Pipeline getPipeline() {
    if (pipeline == null) {
      // pipeline is not set when deserializing, but one is required for eval
      this.pipeline = new MRPipeline(Script.class, name);
    }
    return pipeline;
  }

  public void group() {
    this.lastCollection = group(lastCollection);
  }

  public <E> PCollection<E> addStage(String name, String stageType, Stage stage,
                                     PType<E> resultType) {
    return addStage(
        name, StageType.valueOf(stageType.toUpperCase(Locale.ENGLISH)),
        stage, resultType);
  }

  @SuppressWarnings("unchecked")
  public <E> PCollection<E> addStage(String name, StageType type, Stage stage,
                                     PType<E> resultType) {
    Preconditions.checkNotNull(stage, "Stage implementation is required");
    stages().put(name, stage);

    PCollection<E> result;
    DoFn infected;
    if (type == StageType.REDUCE) {
      PGroupedTable grouped = group(lastCollection);
      infected = infect(carrierTypeFor(type, grouped, stage), name, stage);
      result = grouped.parallelDo(name, infected, resultType);

    } else if (type == StageType.COMBINE) {
      PGroupedTable<?, ?> grouped = group(lastCollection);
      CombineFn infectedCombiner = infectCombiner(name, stage);
      infected = infectedCombiner;
      result = (PCollection<E>) grouped.combineValues(infectedCombiner);

    } else if (type == StageType.PARALLEL) {
      infected = infect(carrierTypeFor(type, lastCollection, stage), name, stage);
      result = lastCollection.parallelDo(name, infected, resultType);

    } else {
      throw new UnsupportedOperationException(
          "[BUG] Invalid stage type: " + type);
    }

    LOG.debug("Instantiated " + infected.getClass() + " for " + name);

    fns().put(name, infected);
    collections().put(name, result);

    this.lastCollection = result;
    return result;
  }

  @SuppressWarnings("unchecked")
  public <S, T> DoFn<S, T> infect(CarrierType type, String name,
                                  Stage stage) {
    switch (type) {
      case FROM_GROUPED_TABLE:
        return new FromGroupedTable(name, this, stage);
      case FROM_TABLE:
        return new FromTable(name, this, stage);
      case FROM_COLLECTION:
        return new FromCollection<S, T>(name, this, stage);
      default:
        throw new IllegalArgumentException(
            "[BUG] Use infectCombiner to instantiate combiners");
    }
  }

  @SuppressWarnings("unchecked")
  public <S, T> CombineFn<S, T> infectCombiner(String name, Stage stage) {
    return new Combiner<S, T>(name, this, stage);
  }

  public PipelineResult run() {
    return getPipeline().done();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("name", name)
        .toString();
  }

  public void ensureEval() {
    if (!evaled) {
      evaled = true;
      try {
        eval();
      } catch (ScriptException e) {
        throw new RuntimeException("Failed to evaluate script: " + name, e);
      }
    }
  }

  private void eval() throws ScriptException {
    System.err.println(String.format("Evaluating script: %s", name));
    System.err.println(String.format("Script \"%s\":%n%s", name,
        Charsets.UTF_8.decode(toByteBuffer())));
    String ext = Iterables.getLast(Splitter.on(".").split(name));
    Evaluator evaluator = Evaluators.get(ext);
    if (evaluator != null) {
      evaluator.eval(this);
    } else {
      throw new RuntimeException(
          "Unknown extension \"" + ext + "\": cannot evaluate \"" + name + "\"");
    }
  }

  @SuppressWarnings("unchecked")
  public static <I> PGroupedTable<?, ?> group(PCollection<I> collection) {
    if (collection instanceof PGroupedTable) {
      return (PGroupedTable<?, ?>) collection;
    } else if (collection instanceof PTable) {
      return ((PTable<?, ?>) collection).groupByKey();
    } else {
      ToTableShim<I> toTable = new ToTableShim<I>(collection);
      return collection
          .parallelDo(toTable, toTable.getTableType()).groupByKey();
    }
  }

  private static CarrierType carrierTypeFor(StageType stage,
                                            PCollection<?> collection,
                                            Stage proc) {
    switch (stage) {
      case COMBINE:
        return CarrierType.COMBINER;
      case REDUCE:
        return CarrierType.FROM_GROUPED_TABLE;
      default:
        if (collection instanceof PGroupedTable) {
          return CarrierType.FROM_GROUPED_TABLE;
        } else if (collection instanceof PTable || proc.arity() == 2) {
          return CarrierType.FROM_TABLE;
        }
        return CarrierType.FROM_COLLECTION;
    }
  }

  public Map<String, DoFn> fns() {
    if (fnsByName == null) {
      fnsByName = Maps.newHashMap();
    }
    return fnsByName;
  }

  @SuppressWarnings("unchecked")
  public <S, T> DoFn<S, T> getDoFn(String name) {
    ensureEval();
    return (DoFn<S, T>) fns().get(name);
  }

  public Map<String, Stage> stages() {
    if (stagesByName == null) {
      stagesByName = Maps.newHashMap();
    }
    return stagesByName;
  }

  @SuppressWarnings("unchecked")
  public <S, T> Stage<S, T> getStage(String name) {
    ensureEval();
    return (Stage<S, T>) stages().get(name);
  }

  public Map<String, PCollection> collections() {
    if (collectionsByName == null) {
      collectionsByName = Maps.newHashMap();
    }
    return collectionsByName;
  }

  @SuppressWarnings("unchecked")
  public <S> PCollection<S> getCollection(String name) {
    ensureEval();
    return (PCollection<S>) collections().get(name);
  }

  private transient Configuration conf = null;

  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    if (conf != null && pipeline != null) {
      pipeline.setConfiguration(conf);
    }
  }

  protected Object writeReplace() throws ObjectStreamException {
    return new StandIn(name, bytes);
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="SE_NO_SERIALVERSIONID",
      justification="Purposely not compatible with other versions")
  private static class StandIn implements Serializable {
    private String name;
    private byte[] bytes;

    public StandIn(String name, byte[] bytes) {
      this.name = name;
      this.bytes = bytes;
      System.err.println("Using script StandIn to serialize \"" + name + "\"");
    }

    private Object readResolve() throws ObjectStreamException {
      System.err.println("Resolving script StandIn \"" + name + "\"");
      return getOrEval(name, bytes);
    }
  }
}
