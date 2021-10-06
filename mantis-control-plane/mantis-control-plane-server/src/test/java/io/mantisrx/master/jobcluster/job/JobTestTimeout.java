/*
 * Copyright 2021 Netflix, Inc.
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
//package io.mantisrx.master.jobcluster.job;
//
//import static org.junit.Assert.assertFalse;
//import static org.junit.Assert.assertTrue;
//import static org.junit.Assert.fail;
//
//import java.time.Instant;
//
//import org.junit.AfterClass;
//import org.junit.BeforeClass;
//import org.junit.Test;
//
//import com.google.common.collect.Lists;
//
//import akka.actor.ActorSystem;
//import akka.testkit.javadsl.TestKit;
//import io.mantisrx.master.jobcluster.job.JobActor.SubscriptionTracker;
//import io.mantisrx.runtime.JobOwner;
//import io.mantisrx.runtime.MachineDefinition;
//import io.mantisrx.runtime.WorkerMigrationConfig;
//import io.mantisrx.runtime.descriptor.SchedulingInfo;
//import io.mantisrx.server.master.domain.IJobClusterDefinition;
//import io.mantisrx.server.master.domain.JobClusterConfig;
//import io.mantisrx.server.master.domain.JobClusterDefinitionImpl;
//import io.mantisrx.server.master.persistence.IMantisStorageProvider;
//import io.mantisrx.server.master.persistence.MantisJobStore;
//import io.mantisrx.server.master.persistence.SimpleCachedFileStorageProvider;
//
//public class JobTestTimeout {
//
//	static ActorSystem system;
//	private static TestKit probe;
//	private static String name;
//	private static MantisJobStore jobStore;
//	private static IMantisStorageProvider storageProvider;
//	private static final String user = "mantis";
//	private static IJobClusterDefinition jobClusterDefn ;
//
//	@BeforeClass
//	public static void setup() {
//		system = ActorSystem.create();
//
//		system = ActorSystem.create();
//		probe = new TestKit(system);
//		name = "testCluster";
//
//		JobClusterConfig clusterConfig = new JobClusterConfig.Builder()
//				.withArtifactName("myart")
//				.withParameters(Lists.newArrayList())
//				.withSchedulingInfo(new SchedulingInfo.Builder().numberOfStages(1).singleWorkerStageWithConstraints(new MachineDefinition(0, 0, 0, 0, 0), Lists.newArrayList(), Lists.newArrayList()).build())
//				.withSubscriptionTimeoutSecs(0)
//				.withVersion("0.0.1")
//
//				.build();
//
//		jobClusterDefn = new JobClusterDefinitionImpl.Builder()
//				.withJobClusterConfig(clusterConfig)
//				.withName(name)
//
//				.withSubscriptionTimeoutSecs(0)
//				.withUser(user)
//				.withIsReadyForJobMaster(true)
//				.withOwner(new JobOwner("Nick", "Mantis", "desc", "nma@netflix.com", "repo"))
//				.withMigrationConfig(WorkerMigrationConfig.DEFAULT)
//
//				.build();
//
//
//		storageProvider = new SimpleCachedFileStorageProvider();
//		jobStore = new MantisJobStore(storageProvider);
//
//
//	}
//
//	@AfterClass
//	public static void tearDown() {
//		((SimpleCachedFileStorageProvider)storageProvider).deleteAllFiles();
//		TestKit.shutdownActorSystem(system);
//		system = null;
//	}
//
//
//
//
//	@Test
//	public void testHasTimedout() {
//		long subsTimeout = 30;
//		long minRuntime = 5;
//		long maxRuntime = Long.MAX_VALUE;
//		SubscriptionTracker st = new SubscriptionTracker(subsTimeout,minRuntime, maxRuntime);
//		Instant now = Instant.now();
//		st.onJobStart(now);
//		// less than min runtime will not timeout
//		assertFalse(st.shouldTerminate(now.plusSeconds(3)));
//
//		// equal to min runtime will not timeout
//		assertFalse(st.shouldTerminate(now.plusSeconds(5)));
//
//		// greater than min runtime and but subscription time out not hit
//		assertFalse(st.shouldTerminate(now.plusSeconds(7)));
//
//		// if it is subscribed then min runtime does not matter
//		st.onSubscribe();
//		assertFalse(st.shouldTerminate(now.plusSeconds(7)));
//
//		st.onUnSubscribe(now.plusSeconds(10));
//		// subs timeout timer will now start
//		// timeout will happen at t + 10 + 30 seconds
//		assertFalse(st.shouldTerminate(now.plusSeconds(32)));
//
//		assertTrue(st.shouldTerminate(now.plusSeconds(40)));
//
//		assertTrue(st.shouldTerminate(now.plusSeconds(42)));
//	}
//
//	@Test
//	public void testMinRuntimeGreater() {
//		long subsTimeout = 30;
//		long minRuntime = 40;
//		long maxRuntime = Long.MAX_VALUE;
//		SubscriptionTracker st = new SubscriptionTracker(subsTimeout, minRuntime, maxRuntime);
//		Instant now = Instant.now();
//		st.onJobStart(now);
//		// less than min runtime will not timeout
//		assertFalse(st.shouldTerminate(now.plusSeconds(35)));
//
//		// equal to min runtime will not timeout
//		assertFalse(st.shouldTerminate(now.plusSeconds(40)));
//
//		// greater than min runtime and  subscription time out  hit
//		assertTrue(st.shouldTerminate(now.plusSeconds(47)));
//
//		// if it is subscribed then min runtime does not matter
//		st.onSubscribe();
//		assertFalse(st.shouldTerminate(now.plusSeconds(47)));
//
//		st.onUnSubscribe(now.plusSeconds(50));
//		// subs timeout timer will now start
//		// timeout will happen at t + 50 + 30 seconds
//		assertFalse(st.shouldTerminate(now.plusSeconds(62)));
//
//		assertTrue(st.shouldTerminate(now.plusSeconds(80)));
//
//		assertTrue(st.shouldTerminate(now.plusSeconds(82)));
//	}
//
//	@Test
//	public void testHasNoTimeoutSet() {
//		long subsTimeout = Long.MAX_VALUE;
//		long minRuntime = 0;
//		long maxRuntime = Long.MAX_VALUE;
//		SubscriptionTracker st = new SubscriptionTracker(subsTimeout, minRuntime, maxRuntime);
//		Instant now = Instant.now();
//		st.onJobStart(now);
//		// less than min runtime will not timeout
//		assertFalse(st.shouldTerminate(now.plusSeconds(3)));
//
//		// equal to min runtime will not timeout
//		assertFalse(st.shouldTerminate(now.plusSeconds(5)));
//
//		// greater than min runtime and but subscription time out not hit
//		assertFalse(st.shouldTerminate(now.plusSeconds(7)));
//
//		// if it is subscribed then min runtime does not matter
//		st.onSubscribe();
//		assertFalse(st.shouldTerminate(now.plusSeconds(7)));
//
//		st.onUnSubscribe(now.plusSeconds(10));
//		// subs timeout timer will now start
//		// timeout will happen at t + 10 + 30 seconds
//		assertFalse(st.shouldTerminate(now.plusSeconds(32)));
//
//		assertFalse(st.shouldTerminate(now.plusSeconds(40)));
//
//		assertFalse(st.shouldTerminate(now.plusSeconds(42)));
//	}
//
//	@Test
//	public void testHasMinRuntimeTimeoutSetOnly() {
//		long subsTimeout = 30;
//		long minRuntime = 5;
//		long maxRuntime = Long.MAX_VALUE;
//		// If subs timeout is not explicitly set it is set to the default of 30
//		SubscriptionTracker st = new SubscriptionTracker(subsTimeout, minRuntime, maxRuntime);
//		Instant now = Instant.now();
//		st.onJobStart(now);
//		// less than min runtime will not timeout
//		assertFalse(st.shouldTerminate(now.plusSeconds(3)));
//
//		// equal to min runtime will not timeout
//		assertFalse(st.shouldTerminate(now.plusSeconds(5)));
//
//		// greater than min runtime and but subscription time out not hit
//		assertFalse(st.shouldTerminate(now.plusSeconds(7)));
//
//		// if it is subscribed then min runtime does not matter
//		st.onSubscribe();
//		assertFalse(st.shouldTerminate(now.plusSeconds(7)));
//
//		st.onUnSubscribe(now.plusSeconds(10));
//		// subs timeout timer will now start
//		// timeout will happen at t + 10 + 30 seconds
//		assertFalse(st.shouldTerminate(now.plusSeconds(32)));
//
//		assertTrue(st.shouldTerminate(now.plusSeconds(40)));
//
//		assertTrue(st.shouldTerminate(now.plusSeconds(42)));
//	}
//
//	@Test
//	public void testHasMaxRuntimeTimeout() {
//		long subsTimeout = 30;
//		long minRuntime = 5;
//		long maxRuntime = 40;
//		// If subs timeout is not explicitly set it is set to the default of 30
//		SubscriptionTracker st = new SubscriptionTracker(subsTimeout, minRuntime, maxRuntime);
//		Instant now = Instant.now();
//		st.onJobStart(now);
//		// less than min runtime will not timeout
//		assertFalse(st.shouldTerminate(now.plusSeconds(3)));
//
//		// equal to min runtime will not timeout
//		assertFalse(st.shouldTerminate(now.plusSeconds(5)));
//
//		// greater than min runtime and but subscription time out not hit
//		assertFalse(st.shouldTerminate(now.plusSeconds(7)));
//
//		// if it is subscribed then min runtime does not matter
//		st.onSubscribe();
//		assertFalse(st.shouldTerminate(now.plusSeconds(7)));
//
//		// max runtime exceeded
//
//		assertFalse(st.shouldTerminate(now.plusSeconds(42)));
//	}
//
//	public void testMaxLessThanMinRuntime() {
//		long subsTimeout = 30;
//		long minRuntime = 5;
//		long maxRuntime = 4;
//		// If subs timeout is not explicitly set it is set to the default of 30
//		try {
//			SubscriptionTracker st = new SubscriptionTracker(subsTimeout, minRuntime, maxRuntime);
//			fail();
//		} catch (IllegalArgumentException e) {
//
//		}
//
//
//	}
//}
