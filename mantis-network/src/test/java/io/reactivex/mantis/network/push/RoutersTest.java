package io.reactivex.mantis.network.push;

import org.junit.jupiter.api.Test;
import rx.functions.Func1;

import static io.reactivex.mantis.network.push.Routers.createRouterInstance;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RoutersTest {

    @Test
    void testCreateRouterInstanceSuccessfully() {
        final Func1<String, byte[]> toBytes = s -> s.getBytes();

        Router<String> router = Routers.createRouterInstance(
            RoundRobinRouter.class.getName(),
            "testRouter",
            toBytes
        );

        assertTrue(router instanceof RoundRobinRouter, "Expected instance of ConsistentHashingRouter");
    }

    @Test
    void testCreateRouterInstanceClassNotFound() {
        final Func1<String, byte[]> toBytes = s -> s.getBytes();
        System.out.println("HEEEEE");

        Router<String> router = createRouterInstance(
            "NonExistentRouterClass",
            "testRouter",
            toBytes
        );

        assertTrue(router instanceof RoundRobinRouter, "Expected instance of ConsistentHashingRouter");
    }
}
