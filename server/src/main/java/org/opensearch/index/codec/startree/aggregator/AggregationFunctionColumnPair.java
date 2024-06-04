/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.codec.startree.aggregator;

import java.util.Comparator;

/** Aggregation function, doc values column pair */
public class AggregationFunctionColumnPair implements Comparable<AggregationFunctionColumnPair> {
    public static final String DELIMITER = "__";
    public static final String STAR = "*";
    public static final AggregationFunctionColumnPair COUNT_STAR = new AggregationFunctionColumnPair(AggregationFunctionType.COUNT, STAR);

    private final AggregationFunctionType _functionType;

    // TODO: Convert to Enum
    private final String _column;

    public AggregationFunctionColumnPair(AggregationFunctionType functionType, String column) {
        _functionType = functionType;
        if (functionType == AggregationFunctionType.COUNT) {
            _column = STAR;
        } else {
            _column = column;
        }
    }

    public AggregationFunctionType getFunctionType() {
        return _functionType;
    }

    public String getColumn() {
        return _column;
    }

    public String toColumnName() {
        return toColumnName(_functionType, _column);
    }

    public static String toColumnName(AggregationFunctionType functionType, String column) {
        return functionType.getName() + DELIMITER + column;
    }

    public static AggregationFunctionColumnPair fromColumnName(String columnName) {
        String[] parts = columnName.split(DELIMITER, 2);
        return fromFunctionAndColumnName(parts[0], parts[1]);
    }

    private static AggregationFunctionColumnPair fromFunctionAndColumnName(String functionName, String columnName) {
        AggregationFunctionType functionType = AggregationFunctionType.getAggregationFunctionType(functionName);
        if (functionType == AggregationFunctionType.COUNT) {
            return COUNT_STAR;
        } else {
            return new AggregationFunctionColumnPair(functionType, columnName);
        }
    }

    @Override
    public int hashCode() {
        return 31 * _functionType.hashCode() + _column.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof AggregationFunctionColumnPair) {
            AggregationFunctionColumnPair anotherPair = (AggregationFunctionColumnPair) obj;
            return _functionType == anotherPair._functionType && _column.equals(anotherPair._column);
        }
        return false;
    }

    @Override
    public String toString() {
        return toColumnName();
    }

    @Override
    public int compareTo(AggregationFunctionColumnPair other) {
        return Comparator.comparing((AggregationFunctionColumnPair o) -> o._column)
            .thenComparing((AggregationFunctionColumnPair o) -> o._functionType)
            .compare(this, other);
    }
}
