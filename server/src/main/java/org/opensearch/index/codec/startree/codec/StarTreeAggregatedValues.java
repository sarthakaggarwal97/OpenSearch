/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
