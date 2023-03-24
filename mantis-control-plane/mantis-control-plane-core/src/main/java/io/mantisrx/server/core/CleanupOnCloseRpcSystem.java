/*
 * Copyright 2023 Netflix, Inc.
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

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.classloading.ComponentClassLoader;
import org.apache.flink.runtime.rpc.AddressResolution;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;

/** An {@link RpcSystem} wrapper that cleans up resources after the RPC system has been closed. */
@Slf4j
class CleanupOnCloseRpcSystem implements RpcSystem {
    private final RpcSystem rpcSystem;
    private final ComponentClassLoader pluginLoader;
    @Nullable private final Path tempDirectory;

    public CleanupOnCloseRpcSystem(
            RpcSystem rpcSystem, ComponentClassLoader pluginLoader, @Nullable Path tempDirectory) {
        this.rpcSystem = Preconditions.checkNotNull(rpcSystem);
        this.pluginLoader = Preconditions.checkNotNull(pluginLoader);
        this.tempDirectory = tempDirectory;
    }

    @Override
    public void close() {
        log.info("Closing MantisAkkaRpcSystemLoader.");
        rpcSystem.close();

        try {
            pluginLoader.close();
        } catch (Exception e) {
            log.warn("Could not close RpcSystem classloader.", e);
        }
        if (tempDirectory != null) {
            try {
                FileUtils.deleteFileOrDirectory(tempDirectory.toFile());
            } catch (Exception e) {
                log.warn("Could not delete temporary rpc system file {}.", tempDirectory, e);
            }
        }
    }

    @Override
    public RpcServiceBuilder localServiceBuilder(Configuration config) {
        return rpcSystem.localServiceBuilder(config);
    }

    @Override
    public RpcServiceBuilder remoteServiceBuilder(
            Configuration configuration,
            @Nullable String externalAddress,
            String externalPortRange) {
        return rpcSystem.remoteServiceBuilder(configuration, externalAddress, externalPortRange);
    }

    @Override
    public String getRpcUrl(
            String hostname,
            int port,
            String endpointName,
            AddressResolution addressResolution,
            Configuration config)
            throws UnknownHostException {
        return rpcSystem.getRpcUrl(hostname, port, endpointName, addressResolution, config);
    }

    @Override
    public InetSocketAddress getInetSocketAddressFromRpcUrl(String url) throws Exception {
        return rpcSystem.getInetSocketAddressFromRpcUrl(url);
    }

    @Override
    public long getMaximumMessageSizeInBytes(Configuration config) {
        return rpcSystem.getMaximumMessageSizeInBytes(config);
    }
}
