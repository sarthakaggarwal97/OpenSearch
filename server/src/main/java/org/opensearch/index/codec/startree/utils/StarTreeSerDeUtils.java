/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.startree.utils;

import org.opensearch.index.codec.startree.aggregator.AvgPair;

import java.nio.ByteBuffer;

public class StarTreeSerDeUtils {

    private StarTreeSerDeUtils() {}

    /**
     * Serializer/De-serializer for a specific type of object.
     *
     * @param <T> Type of the object
     */
    public interface ObjectSerDe<T> {

        /**
         * Serializes a value into a byte array.
         */
        byte[] serialize(T value);

        /**
         * De-serializes a value from a byte array.
         */
        T deserialize(byte[] bytes);

        /**
         * De-serializes a value from a byte buffer.
         */
        T deserialize(ByteBuffer byteBuffer);
    }

    public static final ObjectSerDe<AvgPair> AVG_PAIR_SER_DE = new ObjectSerDe<>() {

        @Override
        public byte[] serialize(AvgPair avgPair) {
            return avgPair.toBytes();
        }

        @Override
        public AvgPair deserialize(byte[] bytes) {
            return AvgPair.fromBytes(bytes);
        }

        @Override
        public AvgPair deserialize(ByteBuffer byteBuffer) {
            return AvgPair.fromByteBuffer(byteBuffer);
        }
    };

}
