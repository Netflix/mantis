/*
 * Copyright 2019 Netflix, Inc.
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
package io.mantisrx.common.network;

/**
 * Defines the set of all configuration for a node in the consistent hash ring
 */
public abstract class NodeConfiguration<T> {

    final int NUM_REPS = 160;

    /**
     * Returns a uniquely identifying key, suitable for hashing by the
     * {@link HashAlgorithm}. A possible implementation for the return value
     * could be: &lt;hostname&gt; + ":" + &lt;port&gt; + "-" +
     * &lt;repetition&gt;
     *
     * @param node
     *            The Node instance to use to form the unique identifier.
     * @param repetition
     *            The repetition number for the particular node in question (0
     *            is the first repetition)
     * @return The key that represents the specific repetition of the node
     */
    public abstract byte[] getKeyForNode(T node, int repetition);

    /**
     * Returns the number of discrete hashes that should be defined for each
     * node in the continuum. For example if each physical node should be
     * represented as 255 virtual nodes in the consistent hash ring, this
     * function should return 255.
     *
     * @return a value greater than 0 (default is 160 as is used in
     *         spy-memcached for {@link HashAlgorithm#KETAMA_HASH}. This is also
     *         used by the evcache code)
     */
    public int getNodeRepetitions() {
        return NUM_REPS;
    }

}
