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

package io.mantisrx.runtime.source.http.impl;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import io.mantisrx.runtime.source.http.ServerPoller;
import mantis.io.reactivex.netty.client.RxClient.ServerInfo;
import org.junit.Test;
import rx.Observable;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;


public class DefaultHttpServerProviderTest {

    @Test
    public void testOverlappingSetsAreHandled() throws Exception {
        final List<ServerInfo> added = new ArrayList<>();
        final Queue<ServerInfo> removed = new ConcurrentLinkedQueue<>();
        final CountDownLatch done = new CountDownLatch(1);
        final int min = 1;
        final int max = 8;
        ServerPoller poller = new ServerPoller() {
            @Override
            public Observable<Set<ServerInfo>> servers() {
                return Observable.range(min, max)
                        .buffer(3, 1)
                        .map(new Func1<List<Integer>, Set<ServerInfo>>() {
                            @Override
                            public Set<ServerInfo> call(List<Integer> ports) {

                                Set<ServerInfo> s = new HashSet<>();
                                for (int port : ports) {
                                    s.add(new ServerInfo("host", port));
                                }

                                return s;
                            }
                        })
                        .doAfterTerminate(new Action0() {
                            @Override
                            public void call() {
                                done.countDown();
                            }
                        });
            }

            @Override
            public Set<ServerInfo> getServers() {
                // TODO Auto-generated method stub
                return null;
            }
        };

        DefaultHttpServerProvider provider = new DefaultHttpServerProvider(poller);

        provider.getServersToAdd()
                .doOnNext(new Action1<ServerInfo>() {
                    @Override
                    public void call(ServerInfo server) {
                        added.add(server);
                    }
                })
                .subscribe();

        provider.getServersToRemove()
                .doOnNext(new Action1<ServerInfo>() {
                    @Override
                    public void call(ServerInfo server) {
                        removed.offer(server);
                    }
                })
                .subscribe();

        done.await();

        int port = min - 1;
        added.sort(new Comparator<ServerInfo>() {
            @Override
            public int compare(ServerInfo o1, ServerInfo o2) {
                return o1.getPort() - o2.getPort();
            }
        });

        for (ServerInfo server : added) {
            port += 1;
            assertEquals(port, server.getPort());
        }
        assertEquals(max, port);

        port = 0;
        for (ServerInfo server : removed) {
            port += 1;
            assertEquals(port, server.getPort());
        }

        assertEquals("The very last element should not be removed. ", max - 1, port);
    }

    @Test
    public void testNewSetAlwaysReplacesOldSet() throws Exception {
        final List<ServerInfo> added = new ArrayList<>();
        final List<ServerInfo> removed = new ArrayList<>();
        final CountDownLatch done = new CountDownLatch(1);
        final int min = 1;
        final int max = 8;
        ServerPoller poller = new ServerPoller() {
            @Override
            public Observable<Set<ServerInfo>> servers() {
                return Observable.range(min, max)
                        .buffer(2)
                        .map(new Func1<List<Integer>, Set<ServerInfo>>() {
                            @Override
                            public Set<ServerInfo> call(List<Integer> ports) {

                                Set<ServerInfo> s = new HashSet<>();
                                for (int port : ports) {
                                    s.add(new ServerInfo("host", port));
                                }

                                return s;
                            }
                        })
                        .doAfterTerminate(new Action0() {
                            @Override
                            public void call() {
                                done.countDown();
                            }
                        });
            }

            @Override
            public Set<ServerInfo> getServers() {
                // TODO Auto-generated method stub
                return null;
            }
        };

        DefaultHttpServerProvider provider = new DefaultHttpServerProvider(poller);

        provider.getServersToAdd()
                .doOnNext(new Action1<ServerInfo>() {
                    @Override
                    public void call(ServerInfo server) {
                        added.add(server);
                    }
                })
                .subscribe();

        provider.getServersToRemove()
                .doOnNext(new Action1<ServerInfo>() {
                    @Override
                    public void call(ServerInfo server) {
                        removed.add(server);
                    }
                })
                .subscribe();

        done.await();

        int port = min - 1;
        // Have to sort because items in a single batch may not come in order
        Collections.sort(added, new Comparator<ServerInfo>() {
            @Override
            public int compare(ServerInfo o1, ServerInfo o2) {
                return o1.getPort() - o2.getPort();
            }
        });

        for (ServerInfo server : added) {
            port += 1;
            assertEquals(port, server.getPort());
        }
        assertEquals(max, port);

        Collections.sort(removed, new Comparator<ServerInfo>() {
            @Override
            public int compare(ServerInfo o1, ServerInfo o2) {
                return o1.getPort() - o2.getPort();
            }
        });
        port = 0;
        for (ServerInfo server : removed) {
            port += 1;
            assertEquals(port, server.getPort());
        }

        assertEquals("The very last two elements should not be removed. ", max - 2, port);
    }

    @Test
    public void testTheSameSetWillBeIdempotent() throws Exception {
        final List<ServerInfo> added = new ArrayList<>();
        final List<ServerInfo> removed = new ArrayList<>();
        final CountDownLatch done = new CountDownLatch(1);
        final int min = 1;
        final int max = 8;
        ServerPoller poller = new ServerPoller() {
            @Override
            public Observable<Set<ServerInfo>> servers() {
                return Observable.range(min, max)
                        .map(new Func1<Integer, Set<ServerInfo>>() {
                            @Override
                            public Set<ServerInfo> call(Integer port) {

                                Set<ServerInfo> s = new HashSet<>();
                                s.add(new ServerInfo("host", 1));
                                s.add(new ServerInfo("host", 2));

                                return s;
                            }
                        })
                        .doAfterTerminate(new Action0() {
                            @Override
                            public void call() {
                                done.countDown();
                            }
                        });
            }

            @Override
            public Set<ServerInfo> getServers() {
                // TODO Auto-generated method stub
                return null;
            }
        };

        DefaultHttpServerProvider provider = new DefaultHttpServerProvider(poller);

        provider.getServersToAdd()
                .doOnNext(new Action1<ServerInfo>() {
                    @Override
                    public void call(ServerInfo server) {
                        added.add(server);
                    }
                })
                .subscribe();

        provider.getServersToRemove()
                .doOnNext(new Action1<ServerInfo>() {
                    @Override
                    public void call(ServerInfo server) {
                        removed.add(server);
                    }
                })
                .subscribe();

        done.await();

        int port = min - 1;
        // Have to sort because items in a single batch may not come in order
        Collections.sort(added, new Comparator<ServerInfo>() {
            @Override
            public int compare(ServerInfo o1, ServerInfo o2) {
                return o1.getPort() - o2.getPort();
            }
        });

        for (ServerInfo server : added) {
            port += 1;
            assertEquals(port, server.getPort());
        }
        assertEquals(2, port);


        assertEquals("No element should be removed. ", 0, removed.size());
    }
}
