/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.codec.startree.aggregator;

/** Count value aggregator */
public class CountValueAggregator implements ValueAggregator<Object, Long> {
    public static final DataType AGGREGATED_VALUE_TYPE = DataType.LONG;

    @Override
    public AggregationFunctionType getAggregationType() {
        return AggregationFunctionType.COUNT;
    }

    @Override
    public DataType getAggregatedValueType() {
        return AGGREGATED_VALUE_TYPE;
    }

    @Override
    public Long getInitialAggregatedValue(Object rawValue) {
        return 1L;
    }

    @Override
    public Long applyRawValue(Long value, Object rawValue) {
        return value + 1;
    }

    @Override
    public Long applyAggregatedValue(Long value, Long aggregatedValue) {
        return value + aggregatedValue;
    }

    @Override
    public Long cloneAggregatedValue(Long value) {
        return value;
    }

    @Override
    public int getMaxAggregatedValueByteSize() {
        return Long.BYTES;
    }

    @Override
    public byte[] serializeAggregatedValue(Long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long deserializeAggregatedValue(byte[] bytes) {
        throw new UnsupportedOperationException();
    }
}
