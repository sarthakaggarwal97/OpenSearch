/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.startree.builder;

import org.opensearch.index.codec.startree.aggregator.AggregationFunctionType;

import java.util.ArrayList;
import java.util.List;

public class Metric {

    private String metricName;
    private List<AggregationFunctionType> aggregationFunctionTypes;

    public Metric(String metricName, List<AggregationFunctionType> aggregationFunctionTypes) {
        this.metricName = metricName;
        this.aggregationFunctionTypes = aggregationFunctionTypes;
    }

    public Metric(String metricName) {
        this.metricName = metricName;
        this.aggregationFunctionTypes = new ArrayList<>();
    }

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public List<AggregationFunctionType> getAggregationFunctionTypes() {
        return aggregationFunctionTypes;
    }

    public void setAggregationFunctionTypes(List<AggregationFunctionType> aggregationFunctionTypes) {
        this.aggregationFunctionTypes = aggregationFunctionTypes;
    }

    public int getNumAggregationFunctionTypes() {
        return aggregationFunctionTypes.size();
    }

}
