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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import javax.script.ScriptException;
import org.apache.crunch.DoFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Source;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.PTableType;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.kitesdk.lang.stages.Stage;
import org.kitesdk.lang.stages.Combiner;
import org.kitesdk.lang.stages.FromCollection;
import org.kitesdk.lang.stages.FromGroupedTable;
import org.kitesdk.lang.stages.FromTable;
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
    COMBINER, FROM_TABLE, FROM_GROUPED_TABLE, FROM_COLLECTION
  }

  private String name;
  private byte[] bytes;

  private transient boolean evaled = false;
  private transient Pipeline pipeline = null;
  private transient PCollection<?> lastCollection = null;
  private transient Stage lastStage = null;
  private transient LinkedHashMap<String, Carrier> carriersByName = null;
  private transient Map<String, DoFn> stagesByName = null;
  private transient Map<String, StageResult> resultsByName = null;

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
    addLastStage();
    getPipeline().write(lastCollection, target);
  }

  public void write(Target target, Target.WriteMode mode) {
    addLastStage();
    getPipeline().write(lastCollection, target, mode);
  }

  public void write(String target) {
    addLastStage();
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
    if (lastStage != null) {
      lastStage.emitPairs();
    }
    this.lastCollection = group(lastCollection);
  }

  public <T> StageResult<T> addCarrier(Carrier<T> infected) {
    Preconditions.checkNotNull(infected, "Carrier is required");
    Preconditions.checkNotNull(lastCollection, "Read must be called first");

    if (infected.type() == Carrier.Type.COMBINE) {
      return addCarrierAsCombiner(infected);
    } else {
      return addCarrierAsStage(infected);
    }
  }

  private <T> StageResult<T> addCarrierAsStage(Carrier<T> infected) {
    String stageName = infected.name();
    carriers().put(stageName, infected);

    addLastStagePaired((infected.type() == Carrier.Type.REDUCE) ||
        (infected.arity() == 2));
    if (infected.type() == Carrier.Type.REDUCE) {
      this.lastCollection = group(lastCollection);
    }

    this.lastStage = makeStage(stageTypeFor(infected, lastCollection), infected);
    stages().put(infected.name(), lastStage);

    StageResult<T> result = new StageResult<T>(stageName);
    results().put(stageName, result);

    return result;
  }

  @SuppressWarnings("unchecked")
  private <S, T> Stage<S, T> makeStage(StageType type, Carrier carrier) {
    switch (type) {
      case FROM_GROUPED_TABLE:
        return new FromGroupedTable(carrier.name(), this, carrier);
      case FROM_TABLE:
        return new FromTable(carrier.name(), this, carrier);
      case FROM_COLLECTION:
        return new FromCollection<S, T>(carrier.name(), this, carrier);
      default:
        throw new IllegalArgumentException(
            "[BUG] Use makeCombiner to instantiate combiners");
    }
  }

  @SuppressWarnings("unchecked")
  private <T> StageResult<T> addCarrierAsCombiner(Carrier<T> infected) {
    Preconditions.checkArgument(infected.type() == Carrier.Type.COMBINE,
        "[BUG] Cannot add type as combiner: " + infected.type());

    addLastStagePaired(true);
    this.lastStage = null; // combiner is added immediately

    Combiner combiner = makeCombiner(infected);
    stages().put(infected.name(), combiner);

    PCollection<T> combined = (PCollection<T>) group(lastCollection)
        .combineValues(combiner);

    this.lastCollection = combined;

    StageResult<T> result = new StageResult<T>(infected.name());
    results().put(infected.name(), result);
    result.setCollection(combined);

    return result;
  }

  @SuppressWarnings("unchecked")
  private Combiner makeCombiner(Carrier carrier) {
    return new Combiner(carrier.name(), this, carrier);
  }

  private void addLastStage() {
    addLastStagePaired(false);
  }

  @SuppressWarnings("unchecked")
  private void addLastStagePaired(boolean emitPairs) {
    if (lastStage != null) {
      if (emitPairs) {
        lastStage.emitPairs();
        this.lastCollection = lastCollection.parallelDo(lastStage.name(),
            lastStage, (PTableType<?, ?>) lastStage.resultType());
      } else {
        this.lastCollection = lastCollection.parallelDo(lastStage.name(),
            lastStage, lastStage.resultType());
      }
      results().get(lastStage.name()).setCollection(lastCollection);
    }
  }

  public PipelineResult run() {
    ensureEval();
    return getPipeline().done();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("name", name)
        .toString();
  }

  private void ensureEval() {
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
      throw new UnsupportedOperationException(
          "Cannot group non-table: " + collection);
    }
  }

  private static StageType stageTypeFor(Carrier carrier,
                                        PCollection<?> collection) {
    switch (carrier.type()) {
      case COMBINE:
        return StageType.COMBINER;
      case REDUCE:
        return StageType.FROM_GROUPED_TABLE;
      default:
        if (collection instanceof PGroupedTable) {
          return StageType.FROM_GROUPED_TABLE;
        } else if (collection instanceof PTable || carrier.arity() == 2) {
          return StageType.FROM_TABLE;
        }
        return StageType.FROM_COLLECTION;
    }
  }

  private Map<String, DoFn> stages() {
    if (stagesByName == null) {
      this.stagesByName = Maps.newHashMap();
    }
    return stagesByName;
  }

  @SuppressWarnings("unchecked")
  public <S, T> DoFn<S, T> getDoFn(String name) {
    ensureEval();
    return (DoFn<S, T>) stages().get(name);
  }

  private LinkedHashMap<String, Carrier> carriers() {
    if (carriersByName == null) {
      this.carriersByName = Maps.newLinkedHashMap();
    }
    return carriersByName;
  }

  @SuppressWarnings("unchecked")
  public <T> Carrier<T> getCarrier(String name) {
    ensureEval();
    return (Carrier<T>) carriers().get(name);
  }

  private Map<String, StageResult> results() {
    if (resultsByName == null) {
      this.resultsByName = Maps.newHashMap();
    }
    return resultsByName;
  }

  @SuppressWarnings("unchecked")
  public <T> StageResult<T> getResult(String name) {
    ensureEval();
    return (StageResult<T>) results().get(name);
  }

  public Configuration getConf() {
    return getPipeline().getConfiguration();
  }

  @Override
  public void setConf(Configuration conf) {
    if (conf != null) {
      getPipeline().setConfiguration(conf);
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
