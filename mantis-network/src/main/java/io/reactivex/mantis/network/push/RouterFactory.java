package io.reactivex.mantis.network.push;

import rx.functions.Func1;

import java.nio.ByteBuffer;

public interface RouterFactory {
    <T> Router<T> scalarStageToStageRouter(String name, final Func1<T, byte[]> toBytes);

    default <T> ProactiveRouter<T> scalarStageToStageProactiveRouter(String name, final Func1<T, byte[]> toBytes) {
        return new ProactiveRoundRobinRouter<>(name, toBytes);
    }

    default <K, V> Router<KeyValuePair<K, V>> keyedRouter(String name, Func1<K, byte[]> keyEncoder, Func1<V, byte[]> valueEncoder) {
        return new ConsistentHashingRouter<>(name, RouterFactory.consistentHashingEncoder(valueEncoder), HashFunctions.xxh3());
    }

    default <K, V> ProactiveRouter<KeyValuePair<K, V>> keyedProactiveRouter(String name, Func1<K, byte[]> keyEncoder, Func1<V, byte[]> valueEncoder) {
        return new ProactiveConsistentHashingRouter<>(name, RouterFactory.consistentHashingEncoder(valueEncoder), HashFunctions.xxh3());
    }

    static <K, V> Func1<KeyValuePair<K, V>, byte[]> consistentHashingEncoder(final Func1<V, byte[]> valueEncoder) {
        return kvp -> {
            byte[] keyBytes = kvp.getKeyBytes();
            byte[] valueBytes = valueEncoder.call(kvp.getValue());
            return
                // length + opcode + notification type + key length
                ByteBuffer.allocate(4 + 1 + 1 + 4 + keyBytes.length + valueBytes.length)
                    .putInt(1 + 1 + 4 + keyBytes.length + valueBytes.length) // length
                    .put((byte) 1) // opcode
                    .put((byte) 1) // notification type
                    .putInt(keyBytes.length) // key length
                    .put(keyBytes) // key bytes
                    .put(valueBytes) // value bytes
                    .array();
        };
    }
}
