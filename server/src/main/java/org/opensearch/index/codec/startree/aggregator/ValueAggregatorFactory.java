/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.codec.startree.aggregator;

/**
 * Value aggregator factory for a given aggregation type
 */
public class ValueAggregatorFactory {
    private ValueAggregatorFactory() {}

    /**
     * Returns a new instance of value aggregator for the given aggregation type.
     *
     * @param aggregationType Aggregation type
     * @return Value aggregator
     */
    public static ValueAggregator getValueAggregator(AggregationFunctionType aggregationType) {
        switch (aggregationType) {
            case COUNT:
                return new CountValueAggregator();
            case SUM:
                return new SumValueAggregator();
            case AVG:
                return new AvgValueAggregator();
            case MAX:
                return new MaxValueAggregator();
            case MIN:
                return new MinValueAggregator();
            default:
                throw new IllegalStateException("Unsupported aggregation type: " + aggregationType);
        }
    }

    /**
     * Returns the data type of the aggregated value for the given aggregation type.
     *
     * @param aggregationType Aggregation type
     * @return Data type of the aggregated value
     */
    public static DataType getAggregatedValueType(AggregationFunctionType aggregationType) {
        switch (aggregationType) {
            case COUNT:
                return CountValueAggregator.AGGREGATED_VALUE_TYPE;
            case SUM:
                return SumValueAggregator.AGGREGATED_VALUE_TYPE;
            // case AVG:
            // return AvgValueAggregator.AGGREGATED_VALUE_TYPE;
            default:
                throw new IllegalStateException("Unsupported aggregation type: " + aggregationType);
        }
    }
}
