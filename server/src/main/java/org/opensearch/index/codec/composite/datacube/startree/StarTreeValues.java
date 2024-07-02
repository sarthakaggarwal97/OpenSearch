/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite.datacube.startree;

import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.codec.composite.CompositeIndexValues;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNode;

import java.util.Map;

/**
 * Concrete class that holds the star tree associated values from the segment
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class StarTreeValues implements CompositeIndexValues {
    private final StarTreeField starTreeField;
    private final StarTreeNode root;
    private final Map<String, DocIdSetIterator> dimensionDocValuesIteratorMap;
    private final Map<String, DocIdSetIterator> metricDocValuesIteratorMap;

    // TODO : come up with full set of vales such as dimensions and metrics doc values + star tree
    public StarTreeValues(List<String> dimensionsOrder) {
        super();
        this.dimensionsOrder = List.copyOf(dimensionsOrder);
    }

    @Override
    public CompositeIndexValues getValues() {
        return this;
    }

    public StarTreeField getStarTreeField() {
        return starTreeField;
    }

    public StarTreeNode getRoot() {
        return root;
    }

    public Map<String, DocIdSetIterator> getDimensionDocValuesIteratorMap() {
        return dimensionDocValuesIteratorMap;
    }

    public Map<String, DocIdSetIterator> getMetricDocValuesIteratorMap() {
        return metricDocValuesIteratorMap;
    }
}
