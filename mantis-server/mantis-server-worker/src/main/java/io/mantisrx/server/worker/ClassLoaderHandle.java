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

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import org.apache.flink.util.UserCodeClassLoader;

/** Handle to retrieve a user code class loader for the associated job. */
interface ClassLoaderHandle extends Closeable {

  /**
   * Gets or resolves the user code class loader for the associated job.
   *
   * <p>In order to retrieve the user code class loader the caller has to specify the required
   * jars and class paths. Upon calling this method first for a job, it will make sure that
   * the required jars are present and potentially cache the created user code class loader.
   * Every subsequent call to this method, will ensure that created user code class loader can
   * fulfill the required jar files and class paths.
   *
   * @param requiredJarFiles requiredJarFiles the user code class loader needs to load
   * @return the user code class loader fulfilling the requirements
   * @throws IOException if the required jar files cannot be downloaded
   * @throws IllegalStateException if the cached user code class loader does not fulfill the
   *     requirements
   */
  UserCodeClassLoader getOrResolveClassLoader(Collection<URI> requiredJarFiles,
      Collection<URL> requiredClasspaths) throws IOException;
}
