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

package io.mantisrx.runtime.source.http;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.nio.NioEventLoopGroup;
import mantis.io.reactivex.netty.client.RxClient.ServerInfo;
import mantis.io.reactivex.netty.protocol.http.server.HttpServer;
import mantis.io.reactivex.netty.protocol.http.server.HttpServerBuilder;
import mantis.io.reactivex.netty.server.RxServerThreadFactory;
import rx.Observable;
import rx.functions.Func1;
import rx.subjects.PublishSubject;


public class LocalServerProvider implements HttpServerProvider {

    private final List<Server> servers = new ArrayList<>();
    private final ConcurrentMap<String, AtomicInteger> subscriptionCount = new ConcurrentHashMap<>();
    private final PublishSubject<ServerInfo> serversToRemove = PublishSubject.create();

    public LocalServerProvider() {
    }

    private List<Server> launchServers(int serverCount, int portStart) {
        List<Server> servers = new ArrayList<>();
        int maxRange = 1000;
        int port = portStart;
        Exception lastException = new Exception("InitialException");
        for (int i = 0; i < serverCount; ++i) {
            int count = 0;
            while (count < maxRange) {
                try {
                    HttpServerBuilder<ByteBuf, ByteBuf> builder = new HttpServerBuilder<>(
                            new ServerBootstrap().group(new NioEventLoopGroup(10, new RxServerThreadFactory())),
                            port,
                            new RequestProcessor());

                    HttpServer<ByteBuf, ByteBuf> server = builder.build();
                    server.start();

                    servers.add(new Server("localhost", port, server));
                    port += 1;
                    break;
                } catch (Exception e) {
                    lastException = e;
                }
            }

            if (count >= maxRange) {
                fail(String.format("Can't obtain %d ports ranging from %d to %d. Last exception: %s",
                        serverCount,
                        portStart,
                        port,
                        lastException.getMessage()));
            }
        }

        return servers;
    }

    @Override
    public Observable<ServerInfo> getServersToAdd() {
        return Observable.from(servers)
                .map(new Func1<Server, ServerInfo>() {
                    @Override
                    public ServerInfo call(Server pair) {
                        return new ServerInfo(pair.getHost(), pair.getPort());
                    }
                });
    }

    public void removeServer(ServerInfo server) {
        serversToRemove.onNext(server);
    }

    @Override
    public Observable<ServerInfo> getServersToRemove() {
        return serversToRemove;
    }

    public int getSubscriptionCount(Server server) {
        return subscriptionCount.get(server.getKey()).get();
    }

    public void shutDown() throws Exception {
        for (Server server : servers) {
            server.getServer().shutdown();
        }
    }

    public void start(int serverCount, int portStart) {
        this.servers.addAll(launchServers(3, portStart));
    }

    public List<Server> getServers() {
        return this.servers;
    }

    public List<ServerInfo> getServerInfos() {
        return Observable.from(this.servers).map(new Func1<Server, ServerInfo>() {
            @Override
            public ServerInfo call(Server server) {
                return new ServerInfo(server.getHost(), server.getPort());
            }
        }).toList().toBlocking().first();
    }

    public int serverSize() {
        return getServers().size();
    }

    public static class Server {

        private final String host;
        private final int port;
        private final HttpServer<ByteBuf, ByteBuf> server;

        public Server(String host, int port, HttpServer<ByteBuf, ByteBuf> server) {
            this.host = host;
            this.port = port;
            this.server = server;
        }

        private static String getKey(String host, int port) {
            return String.format("%s:%d", host, port);
        }

        public static String getKey(ServerInfo server) {
            return getKey(server.getHost(), server.getPort());
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        public HttpServer<ByteBuf, ByteBuf> getServer() {
            return server;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Server server = (Server) o;

            if (port != server.port) return false;
            if (!host.equals(server.host)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = host.hashCode();
            result = 31 * result + port;
            return result;
        }

        public String getKey() {
            return getKey(getHost(), getPort());
        }
    }
}
