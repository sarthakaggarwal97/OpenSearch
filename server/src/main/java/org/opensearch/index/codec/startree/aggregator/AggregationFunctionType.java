/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.codec.startree.aggregator;

/**
 * Aggregated function type
 */
public enum AggregationFunctionType {
    COUNT("count"),
    SUM("sum"),
    MIN("min"),
    MAX("max"),
    AVG("avg");

    private final String name;

    AggregationFunctionType(String name) {
        this.name = name;
    }

    public static AggregationFunctionType getAggregationFunctionType(String functionName) {
        return AggregationFunctionType.valueOf(functionName);
    }

    public String getName() {
        return name;
    }
}
