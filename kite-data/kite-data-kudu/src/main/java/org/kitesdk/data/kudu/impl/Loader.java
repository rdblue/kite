/**
 * Copyright 2015 Cloudera Inc.
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
package org.kitesdk.data.kudu.impl;

import java.util.Map;

import org.kitesdk.data.kudu.KuduDatasetRepository;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.Loadable;
import org.kitesdk.data.spi.OptionBuilder;
import org.kitesdk.data.spi.Registration;
import org.kitesdk.data.spi.URIPattern;

public class Loader implements Loadable {
  @Override
  public void load() {
    Registration.register(
      new URIPattern("kudu::master"),
      new URIPattern("kudu::master/:dataset"),
      new OptionBuilder<DatasetRepository>() {

        @Override
        public DatasetRepository getFromOptions(Map<String, String> options) {
          return new KuduDatasetRepository.Builder().master(options.get("master")).build();
        }
      }
    );
  }
}
