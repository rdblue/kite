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

import com.google.common.base.Throwables;

class RetryUtil {
  private static final int NUM_RETRIES = 5;
  static int SLEEP_BASE_MS = 1000;

  @SuppressWarnings("unchecked")
  public static <R, E extends Exception> R retryWithBackoff(
      Action<R, E> action, Class<E> exc) throws E {
    E last = null;
    for (int i = 0; i < NUM_RETRIES; i += 1) {
      try {
        return action.call();

      } catch (Exception e) {
        if (exc.isAssignableFrom(e.getClass())) {
          last = (E) e;
        } else {
          Throwables.propagate(e);
        }
      }

      if (i + 1 < NUM_RETRIES) { // don't sleep the last time
        try {
          Thread.sleep((long) (SLEEP_BASE_MS * Math.pow(2, i)));
        } catch (InterruptedException e) {
          Thread.interrupted();
        }
      }
    }
    throw last;
  }

  public interface Action<R, E extends Exception> {
    R call() throws E;
  }
}
