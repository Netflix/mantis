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
package io.reactivx.mantis.operators;

public class BufferOnBackPressureOperatorTest {
    //	@Before
    //	public void init() {
    //		System.setProperty("rx.ring-buffer.size","1024");
    //	}
    //	@Test
    //	public void testBufferAndDrop() throws InterruptedException {
    //		final AtomicInteger eventsReceived = new AtomicInteger();
    //    	final CountDownLatch onCompleteReceived = new CountDownLatch(1);
    //		Observable<Integer> es = Observable.create(new Observable.OnSubscribe<Integer>() {
    //
    //            @Override
    //            public void call(final Subscriber<? super Integer> observer) {
    //                System.out.println("*** Subscribing to EventStream ***");
    //
    //                new Thread(new Runnable() {
    //
    //                    @Override
    //                    public void run() {
    //                        for (int i = 0; i < 4000; i++) {
    //
    //                            observer.onNext(i);
    //                        }
    //                        observer.onCompleted();
    //                    }
    //
    //                }).start();
    //            }
    //
    //        });
    //
    //
    //		es.lift(new BufferOnBackPressureOperator<Integer>("a",100))
    //		.observeOn(Schedulers.io())
    //		.subscribe(new Observer<Integer>() {
    //
    //			@Override
    //			public void onCompleted() {
    //				System.out.println("got oncomplete");
    //				onCompleteReceived.countDown();
    //
    //			}
    //
    //			@Override
    //			public void onError(Throwable e) {
    //				e.printStackTrace();
    //
    //			}
    //
    //			@Override
    //			public void onNext(Integer t) {
    //				try {
    //					Thread.sleep(10);
    //				} catch (InterruptedException e) {
    //					// TODO Auto-generated catch block
    //					e.printStackTrace();
    //				}
    ////				System.out.println("got " + t);
    //				eventsReceived.incrementAndGet();
    //
    //			}
    //
    //        });
    //        onCompleteReceived.await();
    //        assertEquals(1124, eventsReceived.get());
    //	}
    //
    //	@Test
    //	public void testOnComplete() throws InterruptedException {
    //		final AtomicInteger eventsReceived = new AtomicInteger();
    //    	final CountDownLatch onCompleteReceived = new CountDownLatch(1);
    //		Observable<Integer> es = Observable.create(new Observable.OnSubscribe<Integer>() {
    //
    //            @Override
    //            public void call(final Subscriber<? super Integer> observer) {
    //                System.out.println("*** Subscribing to EventStream ***");
    //
    //                new Thread(new Runnable() {
    //
    //                    @Override
    //                    public void run() {
    //                        for (int i = 0; i < 1034; i++) {
    //
    //                            observer.onNext(i);
    //                        }
    //                        observer.onCompleted();
    //                    }
    //
    //                }).start();
    //            }
    //
    //        });
    //
    //
    //		es.lift(new BufferOnBackPressureOperator<Integer>("a",100))
    //		.observeOn(Schedulers.io())
    //		.subscribe(new Observer<Integer>() {
    //
    //			@Override
    //			public void onCompleted() {
    //				System.out.println("got oncomplete");
    //				onCompleteReceived.countDown();
    //
    //			}
    //
    //			@Override
    //			public void onError(Throwable e) {
    //				e.printStackTrace();
    //
    //			}
    //
    //			@Override
    //			public void onNext(Integer t) {
    //				try {
    //					Thread.sleep(10);
    //				} catch (InterruptedException e) {
    //					// TODO Auto-generated catch block
    //					e.printStackTrace();
    //				}
    //	//			System.out.println("got " + t);
    //				eventsReceived.incrementAndGet();
    //
    //			}
    //
    //        });
    //        onCompleteReceived.await();
    //        assertEquals(1034, eventsReceived.get());
    //	}
    //
    //	@Test
    //	public void testOnError() throws InterruptedException {
    //		final AtomicInteger eventsReceived = new AtomicInteger();
    //    	final CountDownLatch onErrorReceived = new CountDownLatch(1);
    //
    //		Observable<Integer> es = Observable.create(new Observable.OnSubscribe<Integer>() {
    //
    //            @Override
    //            public void call(final Subscriber<? super Integer> observer) {
    //                System.out.println("*** Subscribing to EventStream ***");
    //
    //                new Thread(new Runnable() {
    //
    //                    @Override
    //                    public void run() {
    //                        for (int i = 0; i < 2000; i++) {
    //
    //                            observer.onNext(i);
    //                            if(i == 1124) {
    //
    //                            	observer.onError(new Exception("forced error"));
    //                            	break;
    //                            }
    //                        }
    //                       // observer.onCompleted();
    //                    }
    //
    //                }).start();
    //            }
    //
    //        });
    //
    //
    //		es.lift(new BufferOnBackPressureOperator<Integer>("a",100))
    //		.observeOn(Schedulers.io())
    //		.subscribe(new Observer<Integer>() {
    //
    //			@Override
    //			public void onCompleted() {
    //
    //
    //			}
    //
    //			@Override
    //			public void onError(Throwable e) {
    //				e.printStackTrace();
    //				onErrorReceived.countDown();
    //			}
    //
    //			@Override
    //			public void onNext(Integer t) {
    //				try {
    //					Thread.sleep(10);
    //				} catch (InterruptedException e) {
    //					// TODO Auto-generated catch block
    //					e.printStackTrace();
    //				}
    //	//			System.out.println("got " + t);
    //				eventsReceived.incrementAndGet();
    //
    //			}
    //
    //        });
    //        onErrorReceived.await();
    ////        assertEquals(1, eventsReceived.get());
    //	}
    //
    //	@Test
    //	public void testNoBufferCompletes() throws InterruptedException {
    //		final AtomicInteger eventsReceived = new AtomicInteger();
    //    	final CountDownLatch onCompleteReceived = new CountDownLatch(1);
    //
    //		Observable<Integer> es = Observable.create(new Observable.OnSubscribe<Integer>() {
    //
    //            @Override
    //            public void call(final Subscriber<? super Integer> observer) {
    //                System.out.println("*** Subscribing to EventStream ***");
    //
    //                new Thread(new Runnable() {
    //
    //                    @Override
    //                    public void run() {
    //                        for (int i = 0; i < 2000; i++) {
    //
    //                            observer.onNext(i);
    //
    //                        }
    //                        observer.onCompleted();
    //                    }
    //
    //                }).start();
    //            }
    //
    //        });
    //
    //
    //		es.lift(new BufferOnBackPressureOperator<Integer>("a",100))
    //
    //		.subscribe(new Observer<Integer>() {
    //
    //			@Override
    //			public void onCompleted() {
    //
    //				onCompleteReceived.countDown();
    //			}
    //
    //			@Override
    //			public void onError(Throwable e) {
    //				e.printStackTrace();
    //
    //			}
    //
    //			@Override
    //			public void onNext(Integer t) {
    //	//			System.out.println("got " + t);
    //				eventsReceived.incrementAndGet();
    //
    //			}
    //
    //        });
    //        onCompleteReceived.await();
    //        assertEquals(2000, eventsReceived.get());
    //	}
    //

}
