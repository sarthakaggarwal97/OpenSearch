/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.codec.startree.aggregator;

import java.nio.ByteBuffer;

public class AvgPair implements Comparable<AvgPair> {

    private double _sum;
    private long _count;

    public AvgPair(double sum, long count) {
        _sum = sum;
        _count = count;
    }

    public void apply(double sum, long count) {
        _sum += sum;
        _count += count;
    }

    public void apply(AvgPair avgPair) {
        _sum += avgPair._sum;
        _count += avgPair._count;
    }

    public double getSum() {
        return _sum;
    }

    public long getCount() {
        return _count;
    }

    public byte[] toBytes() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Double.BYTES + Long.BYTES);
        byteBuffer.putDouble(_sum);
        byteBuffer.putLong(_count);
        return byteBuffer.array();
    }

    public static AvgPair fromBytes(byte[] bytes) {
        return fromByteBuffer(ByteBuffer.wrap(bytes));
    }

    public static AvgPair fromByteBuffer(ByteBuffer byteBuffer) {
        return new AvgPair(byteBuffer.getDouble(), byteBuffer.getLong());
    }

    public int compareTo(AvgPair avgPair) {
        if (_count == 0) {
            if (avgPair._count == 0) {
                return 0;
            } else {
                return -1;
            }
        } else {
            if (avgPair._count == 0) {
                return 1;
            } else {
                double avg1 = _sum / _count;
                double avg2 = avgPair._sum / avgPair._count;
                if (avg1 > avg2) {
                    return 1;
                }
                if (avg1 < avg2) {
                    return -1;
                }
                return 0;
            }
        }
    }
}
