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

package org.kitesdk.scripting;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.kitesdk.compat.DynConstructors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Evaluators {

  private static final Logger LOG = LoggerFactory.getLogger(Evaluators.class);

  private static final Map<String, Evaluator> evaluators = Maps.newHashMap();
  static {
    // dynamically load evaluators
    // Note: this precludes using the maven-shade-plugin minimizeJar feature
    List<String> evaluatorClasses = Lists.newArrayList(
        "org.kitesdk.scripting.RubyEvaluator",
        "org.kitesdk.scripting.PyEvaluator"
    );
    for (String evalClass : evaluatorClasses) {
      try {
        DynConstructors.Ctor<Evaluator> ctor = new DynConstructors.Builder()
            .impl(evalClass)
            .buildChecked();
        Evaluator evaluator = ctor.newInstance();
        evaluators.put(evaluator.ext(), evaluator);
        LOG.info("Loaded evaluator " + evaluator.getClass() + " for " + evaluator.ext());
      } catch (NoSuchMethodException e) {
        // most of the time, at least one language runtime will be missing
        LOG.debug("Cannot load evaluator: " + evalClass);
      }
    }
  }

  public static Evaluator get(String ext) {
    return evaluators.get(ext);
  }
}
