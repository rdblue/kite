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
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import javax.script.ScriptException;
import org.apache.crunch.DoFn;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.kitesdk.compat.DynConstructors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value="SE_NO_SERIALVERSIONID",
    justification="Purposely not compatible with other versions")
public class Script implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(Script.class);

  public static interface Evaluator {
    Analytic eval(Script script) throws ScriptException;

    String ext();
  }

  public static final Map<String, Evaluator> evaluators = Maps.newHashMap();
  static {
    // dynamically load evaluators
    // Note: this precludes using the maven-shade-plugin minimizeJar feature
    List<String> evaluatorClasses = Lists.newArrayList(
        "org.kitesdk.lang.RubyEvaluator",
        "org.kitesdk.lang.PyEvaluator"
    );
    for (String evalClass : evaluatorClasses) {
      try {
        DynConstructors.Ctor<Evaluator> ctor = new DynConstructors.Builder()
            .impl(evalClass)
            .buildChecked();
        Evaluator evaluator = ctor.newInstance();
        evaluators.put(evaluator.ext(), evaluator);
      } catch (NoSuchMethodException e) {
        // most of the time, at least one language runtime will be missing
        LOG.debug("Cannot load evaluator: " + evalClass);
      }
    }
  }

  /*
   * This cache is used to avoid evaluating the script multiple times when
   * this is deserialized. There can be multiple copies of this object, but
   * they will all go through this static cache to eval the analytic bytes.
   */
  private static Cache<String, Analytic> analytics = CacheBuilder.newBuilder().build();

  private String name;
  private byte[] bytes;

  private transient Pipeline pipeline = null;
  private transient Analytic analytic = null;

  public Script(String name, InputStream content) {
    this.name = name;
    try {
      this.bytes = ByteStreams.toByteArray(content);
    } catch (IOException e) {
      throw new IllegalArgumentException("Cannot read script content", e);
    }
  }

  public String getName() {
    return name;
  }

  public ByteBuffer toByteBuffer() {
    // because the buffer itself can have state, create a new one each time
    return ByteBuffer.wrap(bytes).asReadOnlyBuffer();
  }

  public void setPipeline(Pipeline pipeline) {
    this.pipeline = pipeline;
  }

  public Pipeline getPipeline() {
    Preconditions.checkState(pipeline != null, "Pipeline has not been set");
    return pipeline;
  }

  public Analytic ensureEval() {
    if (analytic == null) {
      try {
        this.analytic = analytics.get(name, new Callable<Analytic>() {
          @Override
          public Analytic call() throws ScriptException {
            return eval();
          }
        });
      } catch (ExecutionException ex) {
        throw Throwables.propagate(ex.getCause());
      }
    }
    return analytic;
  }

  public <S, T> DoFn<S, T> getStage(String name) {
    ensureEval();
    return analytic.getStage(name);
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

  private Analytic eval() throws ScriptException {
    System.err.println(String.format("Evaluating script: %s", name));
    System.err.println(String.format("Script \"%s\":%n%s", name,
        Charsets.UTF_8.decode(toByteBuffer())));
    for (String ext : evaluators.keySet()) {
      String dotExt = ext.startsWith(".") ? ext : "." + ext;
      if (name.endsWith(dotExt)) {
        Evaluator evaluator = evaluators.get(ext);
        return evaluator.eval(this);
      }
    }
    throw new RuntimeException(
        "Unknown extension: cannot evaluate \"" + name + "\"");
  }
}
