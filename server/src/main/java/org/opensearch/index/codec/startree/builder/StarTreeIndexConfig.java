/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.opensearch.index.codec.startree.builder;

import org.opensearch.index.codec.startree.aggregator.AggregationFunctionColumnPair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StarTreeIndexConfig {

    // Star-tree will be split with this order (time column is treated as dimension)
    private final List<Dimension> _dimensionsSplitOrder;

    // Do not create star-node for these dimensions
    private final Set<Dimension> _skipStarNodeCreationForDimensions;

    // Function column pairs with delimiter "__", e.g. SUM__col1, MAX__col2, COUNT__*
    private final List<String> _functionColumnPairs;

    // The upper bound of records to be scanned at the leaf node
    private final int _maxLeafRecords;

    private final Map<Dimension, AggregationFunctionColumnPair> _aggregationFunctionColumnPairs;

    public StarTreeIndexConfig(
        List<Dimension> dimensionsSplitOrder,
        Set<Dimension> skipStarNodeCreationForDimensions,
        List<String> functionColumnPairs,
        int maxLeafRecords,
        Map<Dimension, AggregationFunctionColumnPair> aggregationFunctionColumnPairs
    ) {

        if (dimensionsSplitOrder.isEmpty()) {
            throw new IllegalArgumentException("dimensionsSplitOrder must be configured");
        }

        _dimensionsSplitOrder = dimensionsSplitOrder;
        _skipStarNodeCreationForDimensions = skipStarNodeCreationForDimensions;
        _functionColumnPairs = functionColumnPairs;
        _maxLeafRecords = maxLeafRecords;
        _aggregationFunctionColumnPairs = aggregationFunctionColumnPairs;

        if (_functionColumnPairs == null && _aggregationFunctionColumnPairs == null) {
            throw new IllegalArgumentException("functionColumnPairs or aggregationConfigs must be configured");
        }
    }

    public Map<Dimension, AggregationFunctionColumnPair> getAggregationFunctionColumnPairs() {
        return _aggregationFunctionColumnPairs;
    }

    public List<Dimension> getDimensionsSplitOrder() {
        return _dimensionsSplitOrder;
    }

    public Set<Dimension> getSkipStarNodeCreationForDimensions() {
        return _skipStarNodeCreationForDimensions;
    }

    public List<String> getFunctionColumnPairs() {
        return _functionColumnPairs;
    }

    public int getMaxLeafRecords() {
        return _maxLeafRecords;
    }

    public static List<String> getDimensionName(Collection<Dimension> dimensionsSplitOrder) {
        List<String> dimensionNames = new ArrayList<>();
        for (Dimension dimension : dimensionsSplitOrder) {
            dimensionNames.add(dimension.getDimensionName());
        }
        return dimensionNames;
    }

}
