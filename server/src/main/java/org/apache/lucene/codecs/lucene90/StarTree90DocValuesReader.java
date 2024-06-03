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
package org.apache.lucene.codecs.lucene90;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.IndexInput;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.codec.StarTreeReader;
import org.opensearch.index.codec.startree.builder.StarTreeIndexConfig;
import org.opensearch.index.codec.startree.codec.StarTreeAggregatedValues;
import org.opensearch.index.codec.startree.node.OffHeapStarTree;
import org.opensearch.index.codec.startree.node.StarTree;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat.DATA_CODEC;
import static org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat.META_CODEC;

/**
 * Custom star tree doc values reader
 */
public class StarTree90DocValuesReader extends DocValuesProducer implements StarTreeReader {
    private final DocValuesProducer delegate;

    private final StarTree90DocValuesProducer valuesProducer;

    StarTree starTree;

    Map<String, SortedNumericDocValues> dimensionValues;

    Map<String, SortedNumericDocValues> metricValues;

    public StarTree90DocValuesReader(DocValuesProducer producer, SegmentReadState state, List<StarTreeIndexConfig> starTreeIndexConfigs)
        throws IOException {
        this.delegate = producer;
        String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, "stttree");
        IndexInput data = state.directory.openInput(dataName, state.context);
        CodecUtil.checkIndexHeader(data, CodecService.STAR_TREE_CODEC, 0, 0, state.segmentInfo.getId(), state.segmentSuffix);
        starTree = new OffHeapStarTree(data);
        valuesProducer = new StarTree90DocValuesProducer(state, DATA_CODEC, "sttd", META_CODEC, "sttm", starTree.getDimensionNames());
        dimensionValues = new LinkedHashMap<>();
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        return delegate.getNumeric(field);
    }

    public StarTreeAggregatedValues getAggregatedDocValues() throws IOException {
        List<String> dimensionsSplitOrder = starTree.getDimensionNames();
        for (int i = 0; i < dimensionsSplitOrder.size(); i++) {
            dimensionValues.put(dimensionsSplitOrder.get(i), valuesProducer.getSortedNumeric(dimensionsSplitOrder.get(i) + "_dim"));
        }
        metricValues = new LinkedHashMap<>();
        metricValues.put("status_sum", valuesProducer.getSortedNumeric("status_sum_metric"));
        // metricValues.put("status_count", valuesProducer.getNumeric("status_count_metric"));
        return new StarTreeAggregatedValues(starTree, dimensionValues, metricValues);
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        return delegate.getBinary(field);
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
        return delegate.getSorted(field);
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        return delegate.getSortedNumeric(field);
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        return delegate.getSortedSet(field);
    }

    @Override
    public void checkIntegrity() throws IOException {}

    @Override
    public void close() throws IOException {}

    @Override
    public StarTreeAggregatedValues getStarTreeValues() throws IOException {
        return getAggregatedDocValues();
    }
}
