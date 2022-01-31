/*
 * Copyright 2022 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.server.worker;

import java.net.URL;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.classloading.ComponentClassLoader;

public class SubmoduleClassLoader extends ComponentClassLoader {
  public SubmoduleClassLoader(URL[] classpath, ClassLoader parentClassLoader) {
    super(
        classpath,
        parentClassLoader,
        CoreOptions.parseParentFirstLoaderPatterns(
            "org.slf4j;org.apache.log4j;org.apache.logging;org.apache.commons.logging;ch.qos.logback", ""),
        new String[] {"org.apache.flink", "io.mantisrx"});
  }
}
