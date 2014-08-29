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

package org.kitesdk.data.mapreduce;


import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.data.TestHelpers;

public class TestRetry {

  private static class TestAction<E> implements RetryUtil.Action<E, RuntimeException> {
    private final E value;
    private final int numFailures;
    private int numCalls;

    private TestAction(E value, int numFailures) {
      this.value = value;
      this.numFailures = numFailures;
    }

    @Override
    public E call() throws RuntimeException {
      numCalls += 1;
      if (numCalls <= numFailures) {
        throw new RuntimeException("Failure #" + numCalls);
      }
      return value;
    }
  }

  @Test
  public void testInitialSuccess() {
    RetryUtil.SLEEP_BASE_MS = 100;

    String expected = "initial success";
    TestAction<String> action = new TestAction<String>(expected, 0);

    long start = System.currentTimeMillis();
    String actual = RetryUtil.retryWithBackoff(action, RuntimeException.class);
    long finish = System.currentTimeMillis();

    Assert.assertEquals("Should return the value unmodified", expected, actual);
    Assert.assertTrue("Should not sleep", (finish - start) < 80);
  }

  @Test
  public void testEventualSuccess() {
    RetryUtil.SLEEP_BASE_MS = 100;

    String expected = "eventual success";
    TestAction<String> action = new TestAction<String>(expected, 4);

    long start = System.currentTimeMillis();
    String actual = RetryUtil.retryWithBackoff(action, RuntimeException.class);
    long finish = System.currentTimeMillis();
    long duration = (finish - start);

    Assert.assertEquals("Should return the value unmodified", expected, actual);
    Assert.assertTrue("Should sleep 4 times (~1500ms)", // 1 + 2 + 4 + 8
        (1400 < duration) && (duration < 1600));
  }

  @Test
  public void testEventualFailure() {
    RetryUtil.SLEEP_BASE_MS = 100;

    final TestAction<String> action = new TestAction<String>("failure", 6);

    long start = System.currentTimeMillis();
    TestHelpers.assertThrows("Should fail 5 times and stop retrying",
        RuntimeException.class, new Runnable() {
          @Override
          public void run() {
            RetryUtil.retryWithBackoff(action, RuntimeException.class);
          }
        });
    long finish = System.currentTimeMillis();
    long duration = (finish - start);

    Assert.assertTrue("Should sleep 4 times (~1500ms)", // 1 + 2 + 4 + 8
        (1400 < duration) && (duration < 1600));
  }

}
