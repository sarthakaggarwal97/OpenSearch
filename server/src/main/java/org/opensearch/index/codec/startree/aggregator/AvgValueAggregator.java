/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.codec.startree.aggregator;

import org.opensearch.index.codec.startree.utils.StarTreeSerDeUtils;

public class AvgValueAggregator implements ValueAggregator<Object, AvgPair> {
    public static final DataType AGGREGATED_VALUE_TYPE = DataType.BYTES;

    @Override
    public AggregationFunctionType getAggregationType() {
        return AggregationFunctionType.AVG;
    }

    @Override
    public DataType getAggregatedValueType() {
        return AGGREGATED_VALUE_TYPE;
    }

    @Override
    public AvgPair getInitialAggregatedValue(Object rawValue) {
        if (rawValue instanceof byte[]) {
            return deserializeAggregatedValue((byte[]) rawValue);
        } else {
            return new AvgPair(((Number) rawValue).doubleValue(), 1L);
        }
    }

    @Override
    public AvgPair applyRawValue(AvgPair value, Object rawValue) {
        if (rawValue instanceof byte[]) {
            value.apply(deserializeAggregatedValue((byte[]) rawValue));
        } else {
            value.apply(((Number) rawValue).doubleValue(), 1L);
        }
        return value;
    }

    @Override
    public AvgPair applyAggregatedValue(AvgPair value, AvgPair aggregatedValue) {
        value.apply(aggregatedValue);
        return value;
    }

    @Override
    public AvgPair cloneAggregatedValue(AvgPair value) {
        return new AvgPair(value.getSum(), value.getCount());
    }

    @Override
    public int getMaxAggregatedValueByteSize() {
        return Double.BYTES + Long.BYTES;
    }

    @Override
    public byte[] serializeAggregatedValue(AvgPair value) {
        return StarTreeSerDeUtils.AVG_PAIR_SER_DE.serialize(value);
    }

    @Override
    public AvgPair deserializeAggregatedValue(byte[] bytes) {
        return StarTreeSerDeUtils.AVG_PAIR_SER_DE.deserialize(bytes);
    }
}
