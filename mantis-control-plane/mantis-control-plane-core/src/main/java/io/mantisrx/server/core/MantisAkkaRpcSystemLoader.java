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

package io.mantisrx.server.core;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.rpc.RpcSystemLoader;
import org.apache.flink.runtime.rpc.akka.AkkaRpcSystem;

/**
 * RpcSystemLoader for mantis task executor and other services that need to expose an RPC API.
 * This particular implementation uses the akka RPC implementation under the hood.
 */
public class MantisAkkaRpcSystemLoader implements RpcSystemLoader {

    @Override
    public RpcSystem loadRpcSystem(Configuration config) {
        return new AkkaRpcSystem();
    }

    public static RpcSystem load(Configuration configuration) {
        return new MantisAkkaRpcSystemLoader().loadRpcSystem(configuration);
    }
}
