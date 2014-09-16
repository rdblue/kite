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

package org.kitesdk.data.spi.filesystem;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.compat.DynConstructors;
import org.kitesdk.compat.DynMethods;
import org.kitesdk.compat.Hadoop;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.spi.DefaultConfiguration;

public class TestProvidedConfiguration {

  private static boolean keepRunning = true;
  private static boolean ready = false;
  private static Thread miniClusterThread = null;

  @BeforeClass
  public static void startMiniCluster() throws Exception {
    /* This mess starts a MiniDFSCluster in a completely separate ClassLoader.
     *
     * All of the classes in the new ClassLoader are not recognized as the same
     * class by java, so the Configuration for the MiniCluster can look for
     * hdfs-site.xml without affecting the Configuration in the test, which is
     * testing that hdfs-site.xml is correctly loaded if it was not already.
     */

    ClassLoader loader = AccessController.doPrivileged(
        new CopyContextClassLoader());

    final DynConstructors.Ctor<Object> confCtor = new DynConstructors.Builder()
        .loader(loader)
        .impl("org.apache.hadoop.conf.Configuration")
        .build();

    final Class<?> soClass = loader.loadClass(
        "org.apache.hadoop.hdfs.server.common.HdfsServerConstants$StartupOption");

    final DynConstructors.Ctor<Object> clusterCtor = new DynConstructors.Builder()
        .loader(loader)
        .impl("org.apache.hadoop.hdfs.MiniDFSCluster",
            int.class, confCtor.getConstructedClass(), int.class,
            boolean.class, boolean.class, soClass, String[].class)
        .build();

    final DynMethods.UnboundMethod shutdown = new DynMethods.Builder("shutdown")
        .loader(loader)
        .impl("org.apache.hadoop.hdfs.MiniDFSCluster")
        .build();

    miniClusterThread = new Thread(new Runnable() {
      @Override
      public void run() {
        Object cluster = clusterCtor.newInstance(
            18020, confCtor.newInstance(), 1, true, true, null, null);

        ready = true;

        while (keepRunning) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException _) {
            Thread.interrupted();
          }
        }

        shutdown.invoke(cluster);
      }
    });

    // set the context ClassLoader for the thread so classes are compatible
    miniClusterThread.setContextClassLoader(loader);
    miniClusterThread.start();

    while (!ready) {
      Thread.sleep(1000);
    }
  }

  @AfterClass
  public static void shutdownMiniCluster() throws Exception {
    keepRunning = false;
    miniClusterThread.join();
  }

  @Test
  public void testHdfsSiteCorrectlyLoaded() throws IOException {
    //Assume.assumeTrue(!Hadoop.isHadoop1()); // hdfs-site.xml is not used

    // get the default config
    Configuration c = DefaultConfiguration.get();
    final File hdfsXml = new File("/tmp/test-classpath/hdfs-site.xml");

    // drop a Configuration into the CLASSPATH
    Configuration hdfsSite = new Configuration(false);
    hdfsSite.set("fs.defaultFS", "hdfs://localhost:18020");
    hdfsSite.setBoolean("kite.config-is-present", true);

    ClassLoader original = Thread.currentThread().getContextClassLoader();

    try {
      // /tmp/test-classpath is added to the maven-surefire-plugin
      OutputStream out = new FileOutputStream(hdfsXml, true);
      hdfsSite.writeXml(out);
      out.close();

      // create a fake ClassLoader that can find the test hdfs-site.xml config
      Thread.currentThread().setContextClassLoader(
          new ClassLoader(original) {
            @Override
            protected URL findResource(String name) {
              if ("hdfs-site.xml".equals(name)) {
                try {
                  return hdfsXml.toURI().toURL();
                } catch (MalformedURLException _) {
                  return null;
                }
              } else {
                return null;
              }
            }
          });

      // verify that the hdfs-site.xml file has not been loaded
      Assert.assertFalse(
          "Configuration has already loaded hdfs-site.xml from the classpath",
          c.getBoolean("kite.config-is-present", false));

      Assert.assertFalse("Dataset should not already exist",
          Datasets.exists("dataset:hdfs:/tmp/data/ns/test"));

      Assert.assertTrue(
          "Configuration should have loaded hdfs-site.xml from the classpath",
          c.getBoolean("kite.config-is-present", false));

      Datasets.create("dataset:hdfs:/tmp/data/ns/test",
          new DatasetDescriptor.Builder()
              .schemaLiteral("\"string\"")
              .build());

      Datasets.delete("dataset:hdfs:/tmp/data/ns/test");

    } finally {
      Thread.currentThread().setContextClassLoader(original);
      hdfsXml.delete();
    }
  }

  private static class CopyContextClassLoader implements PrivilegedAction<ClassLoader> {
    @Override
    public ClassLoader run() {
      ClassLoader current = Thread.currentThread().getContextClassLoader();
      if (current instanceof URLClassLoader) {
        URLClassLoader toCopy = (URLClassLoader) current;
        return new URLClassLoader(
            toCopy.getURLs(), toCopy.getParent());
      } else {
        throw new RuntimeException("Cannot copy context ClassLoader");
      }
    }
  }
}
