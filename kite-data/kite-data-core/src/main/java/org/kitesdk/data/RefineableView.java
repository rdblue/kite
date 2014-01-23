/*
 * Copyright 2013 Cloudera.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data;

public interface RefineableView<E> extends View<E> {

  RefineableView<E> with(String name);

  RefineableView<E> with(String name, Object value);

  RefineableView<E> from(String name, Object value);
  RefineableView<E> from(String[] names, Object... values);

  RefineableView<E> fromAfter(String name, Object value);
  RefineableView<E> fromAfter(String[] names, Object... values);

  RefineableView<E> to(String name, Object value);
  RefineableView<E> to(String[] names, Object... values);

  RefineableView<E> toBefore(String name, Object value);
  RefineableView<E> toBefore(String[] names, Object... values);

  RefineableView<E> of(String[] names, Object... values);

  RefineableView<E> union(View<E> other);

  RefineableView<E> complement();

}
