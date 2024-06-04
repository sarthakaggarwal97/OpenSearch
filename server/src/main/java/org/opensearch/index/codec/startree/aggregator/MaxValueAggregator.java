/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.codec.startree.aggregator;

public class MaxValueAggregator implements ValueAggregator<Number, Double> {
    public static final DataType AGGREGATED_VALUE_TYPE = DataType.DOUBLE;

    @Override
    public AggregationFunctionType getAggregationType() {
        return AggregationFunctionType.MAX;
    }

    @Override
    public DataType getAggregatedValueType() {
        return AGGREGATED_VALUE_TYPE;
    }

    @Override
    public Double getInitialAggregatedValue(Number rawValue) {
        return rawValue.doubleValue();
    }

    @Override
    public Double applyRawValue(Double value, Number rawValue) {
        return Math.max(value, rawValue.doubleValue());
    }

    @Override
    public Double applyAggregatedValue(Double value, Double aggregatedValue) {
        return Math.max(value, aggregatedValue);
    }

    @Override
    public Double cloneAggregatedValue(Double value) {
        return value;
    }

    @Override
    public int getMaxAggregatedValueByteSize() {
        return Double.BYTES;
    }

    @Override
    public byte[] serializeAggregatedValue(Double value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Double deserializeAggregatedValue(byte[] bytes) {
        throw new UnsupportedOperationException();
    }
}
