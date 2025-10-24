package io.reactivex.mantis.network.push;

import rx.functions.Func1;

import java.nio.ByteBuffer;

public interface RouterFactory {
    public <T> Router<T> scalarStageToStageRouter(String name, final Func1<T, byte[]> toBytes);

    public default <K, V> Router<KeyValuePair<K, V>> keyedRouter(String name,
                                                                 final Func1<K, byte[]> keyEncoder,
                                                                 final Func1<V, byte[]> valueEncoder) {
        return new ConsistentHashingRouter<K, V>(name, new Func1<KeyValuePair<K, V>, byte[]>() {
            @Override
            public byte[] call(KeyValuePair<K, V> kvp) {
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
            }
        }, HashFunctions.xxh3());
    };
}
