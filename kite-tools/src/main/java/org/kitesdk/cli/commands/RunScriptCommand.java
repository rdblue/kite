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

package org.kitesdk.cli.commands;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.util.DistCache;
import org.kitesdk.compat.DynMethods;
import org.kitesdk.lang.Script;
import org.slf4j.Logger;

@Parameters(commandDescription="Run a script on a dataset")
public class RunScriptCommand extends BaseCommand {

  private final Logger console;

  public RunScriptCommand(Logger console) {
    this.console = console;
  }

  @Parameter(description="<script>")
  List<String> scripts;

  @Parameter(names = {"--in-memory"}, hidden = true,
      description = "If set, use a memory pipeline.")
  boolean mem = false;

  @Parameter(names="--jar",
      description="Add a jar to the runtime classpath")
  List<String> jars;

  @Override
  public int run() throws IOException {
    Preconditions.checkArgument(scripts != null && !scripts.isEmpty(),
        "No script specified");
    Preconditions.checkArgument(scripts.size() == 1,
        "Only one script can specified");

    // set up dependencies
    ClassLoader current = getClass().getClassLoader();
    DynMethods.BoundMethod addJar = new DynMethods.Builder("addURL")
        .hiddenImpl(URLClassLoader.class, URL.class)
        .build(current);

    if (jars != null && !jars.isEmpty()) {
      for (String jar : jars) {
        File jarFile = new File(jar);
        DistCache.addJarToDistributedCache(getConf(), jarFile);
        // add to the current loader
        addJar.invoke(jarFile.toURI().toURL());
      }
    }

    String scriptName = scripts.get(0);
    Script script = new Script(scriptName, open(scriptName));
    console.info("Running script...");

    script.setPipeline(mem ? MemPipeline.getInstance():
        new MRPipeline(Script.class, script.getName()));

    try {
      script.ensureEval();
    } catch (Exception e) {
      console.error("Error while evaluating script", e);
    }

    try {
      return script.run().succeeded() ? 0 : 1;
    } catch (Exception e) {
      console.error("Error while running job", e);
      return 1;
    }
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# run ruby word count script:",
        "word-count.rb"
    );
  }
}
