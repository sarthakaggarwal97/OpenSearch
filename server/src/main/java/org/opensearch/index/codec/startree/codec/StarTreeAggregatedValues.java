/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.codec.startree.codec;

import org.apache.lucene.index.SortedNumericDocValues;
import org.opensearch.index.codec.startree.node.StarTree;

import java.util.Map;

// TODO : this is tightly coupled to star tree

/**
 * Star tree aggregated values holder for reader / query
 * */
public class StarTreeAggregatedValues {
    public StarTree _starTree;

    // TODO: Based on the implementation, these NEED to be INORDER or implementation of LinkedHashMap
    public Map<String, SortedNumericDocValues> dimensionValues;
    public Map<String, SortedNumericDocValues> metricValues;

    public StarTreeAggregatedValues(
        StarTree starTree,
        Map<String, SortedNumericDocValues> dimensionValues,
        Map<String, SortedNumericDocValues> metricValues
    ) {
        this._starTree = starTree;
        this.dimensionValues = dimensionValues;
        this.metricValues = metricValues;
    }
}
