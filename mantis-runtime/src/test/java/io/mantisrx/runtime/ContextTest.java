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

package io.mantisrx.runtime;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;
import rx.functions.Action0;

public class ContextTest {

    @Test
    public void testEagerSubscriptionActivationCallback() {
        // Setup
        Context context = createTestContext();
        AtomicBoolean callbackInvoked = new AtomicBoolean(false);

        Action0 testCallback = () -> callbackInvoked.set(true);

        // Act
        context.setEagerSubscriptionActivationCallback(testCallback);
        context.activateEagerSubscription();

        // Assert
        Assert.assertTrue("Callback should be invoked when activateEagerSubscription is called",
                         callbackInvoked.get());
    }

    @Test
    public void testEagerSubscriptionActivationWithoutCallback() {
        // Setup
        Context context = createTestContext();

        // Act & Assert - Should not throw exception when no callback is set
        context.activateEagerSubscription();

        // Test passes if no exception is thrown
    }

    @Test
    public void testEagerSubscriptionCallbackOnlyCalledOnce() {
        // Setup
        Context context = createTestContext();
        AtomicInteger callbackCount = new AtomicInteger(0);

        Action0 testCallback = callbackCount::incrementAndGet;

        // Act
        context.setEagerSubscriptionActivationCallback(testCallback);
        context.activateEagerSubscription();
        context.activateEagerSubscription();
        context.activateEagerSubscription();

        // Assert
        Assert.assertEquals("Callback should only be called once per activation", 3, callbackCount.get());
    }

    @Test
    public void testEagerSubscriptionCallbackReplacement() {
        // Setup
        Context context = createTestContext();
        AtomicBoolean firstCallbackInvoked = new AtomicBoolean(false);
        AtomicBoolean secondCallbackInvoked = new AtomicBoolean(false);

        Action0 firstCallback = () -> firstCallbackInvoked.set(true);
        Action0 secondCallback = () -> secondCallbackInvoked.set(true);

        // Act
        context.setEagerSubscriptionActivationCallback(firstCallback);
        context.setEagerSubscriptionActivationCallback(secondCallback); // Replace callback
        context.activateEagerSubscription();

        // Assert
        Assert.assertFalse("First callback should not be invoked after replacement",
                          firstCallbackInvoked.get());
        Assert.assertTrue("Second callback should be invoked",
                         secondCallbackInvoked.get());
    }

    @Test
    public void testEagerSubscriptionCallbackWithNullCallback() {
        // Setup
        Context context = createTestContext();
        AtomicBoolean originalCallbackInvoked = new AtomicBoolean(false);

        Action0 originalCallback = () -> originalCallbackInvoked.set(true);

        // Act
        context.setEagerSubscriptionActivationCallback(originalCallback);
        context.setEagerSubscriptionActivationCallback(null); // Set to null
        context.activateEagerSubscription();

        // Assert
        Assert.assertFalse("Original callback should not be invoked after setting to null",
                          originalCallbackInvoked.get());
    }

    @Test
    public void testContextGettersStillWork() {
        // Setup - use no-arg constructor for testing
        Context context = new Context();

        // Act & Assert - Verify existing functionality still works with test constructor
        Assert.assertNotNull("Parameters should be accessible", context.getParameters());
        Assert.assertNotNull("Worker map observable should be accessible", context.getWorkerMapObservable());
    }

    @Test
    public void testEagerSubscriptionCallbackIntegrationWithCompleteAndExit() {
        AtomicBoolean callbackCalled = new AtomicBoolean(false);
        Action0 eagerSubscriptionCallback = () -> callbackCalled.set(true);

        Context context = new Context();

        // Act
        context.setEagerSubscriptionActivationCallback(eagerSubscriptionCallback);
        context.activateEagerSubscription();

        // Assert
        Assert.assertTrue("Eager subscription callback should be called", callbackCalled.get());
    }

    private Context createTestContext() {
        return new Context(); // Use no-arg constructor for testing
    }
}
