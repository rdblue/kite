/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.scripting.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Set;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.util.DistCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.PropertyConfigurator;
import org.kitesdk.compat.DynMethods;
import org.kitesdk.scripting.Script;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Parameters(commandDescription="Run kite scripts")
public class Main extends Configured implements Tool {

  @Parameter(names = {"-v", "--verbose", "--debug"},
      description = "Print extra debugging information")
  private boolean debug = false;

  @Parameter(description="<script> <script args>")
  List<String> targets;

  @Parameter(names = {"--in-memory"}, hidden = true,
      description = "If set, use a memory pipeline.")
  boolean mem = false;

  @Parameter(names="--jar",
      description="Add a jar to the runtime classpath")
  List<String> jars;

  @VisibleForTesting
  static final String PROGRAM_NAME = "script";

  private static Set<String> HELP_ARGS = ImmutableSet.of("-h", "-help", "--help", "help");

  private final Logger console;

  private FileSystem localFS = null;

  @VisibleForTesting
  final JCommander jc;

  Main(Logger console) {
    this.console = console;
    this.jc = new JCommander(this);
    jc.setProgramName(PROGRAM_NAME);
  }

  @Override
  public int run(String[] args) throws Exception {
    jc.parse(args);

    // configure log4j
    if (debug) {
      org.apache.log4j.Logger console = org.apache.log4j.Logger.getLogger(Main.class);
      console.setLevel(Level.DEBUG);
    }

    Preconditions.checkArgument(targets != null && !targets.isEmpty(),
        "No script specified");
    Preconditions.checkArgument(targets.size() == 1,
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

    String scriptName = targets.get(0);
    console.info("Running script...");

    Script script = Script.get(scriptName, open(scriptName));

    script.setPipeline(mem ? MemPipeline.getInstance():
        new MRPipeline(Script.class, script.getName()));

    script.setConf(getConf());

    try {
      return script.run().succeeded() ? 0 : 1;
    } catch (Exception e) {
      console.error("Error while running job", e);
      return 1;
    }
  }

  /**
   * @return FileSystem to use when no file system scheme is present in a path
   * @throws IOException
   */
  public FileSystem defaultFS() throws IOException {
    if (localFS == null) {
      this.localFS = FileSystem.getLocal(getConf());
    }
    return localFS;
  }

  /**
   * Returns a qualified {@link Path} for the {@code filename}.
   *
   * If the file does not have a file system scheme, this uses the default FS.
   *
   * @param filename The filename to qualify
   * @return A qualified Path for the filename
   * @throws IOException
   */
  public Path qualifiedPath(String filename) throws IOException {
    Path cwd = defaultFS().makeQualified(new Path("."));
    return new Path(filename).makeQualified(defaultFS().getUri(), cwd);
  }

  /**
   * Opens an existing file or resource.
   *
   * If the file does not have a file system scheme, this uses the default FS.
   *
   * @param filename The filename to open.
   * @return An open InputStream with the file contents
   * @throws java.io.IOException
   * @throws IllegalArgumentException If the file does not exist
   */
  public InputStream open(String filename) throws IOException {
    Path filePath = qualifiedPath(filename);
    // even though it was qualified using the default FS, it may not be in it
    FileSystem fs = filePath.getFileSystem(getConf());
    if (fs.exists(filePath)) {
      return fs.open(filePath);
    } else {
      try {
        return Resources.getResource(filename).openStream();
      } catch (IllegalArgumentException e) {
        // not a resource, throw an exception with a better error message
        throw new IllegalArgumentException("Path does not exist: " + filePath);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    // reconfigure logging with the kite CLI configuration
    PropertyConfigurator.configure(
        Main.class.getResource("/kite-scripting-logging.properties"));
    Logger console = LoggerFactory.getLogger(Main.class);
    int rc = ToolRunner.run(new Configuration(), new Main(console), args);
    System.exit(rc);
  }
}
