/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.codecs.lucene90;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StarTree90DocValuesProducer extends DocValuesProducer {

    Lucene90DocValuesProducer lucene90DocValuesProducer;
    private final List<String> dimensionSplitOrder;
    private final FieldInfos fieldInfos;

    public StarTree90DocValuesProducer(
        SegmentReadState state,
        String dataCodec,
        String dataExtension,
        String metaCodec,
        String metaExtension,
        List<String> dimensionSplitOrder
    ) throws IOException {
        this.dimensionSplitOrder = dimensionSplitOrder;
        this.fieldInfos = new FieldInfos(getFieldInfoList());
        SegmentReadState segmentReadState = new SegmentReadState(state.directory, state.segmentInfo, fieldInfos, state.context);
        lucene90DocValuesProducer = new Lucene90DocValuesProducer(segmentReadState, dataCodec, dataExtension, metaCodec, metaExtension);
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        return this.lucene90DocValuesProducer.getNumeric(field);
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        return this.lucene90DocValuesProducer.getBinary(field);
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
        return this.lucene90DocValuesProducer.getSorted(field);
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        return this.lucene90DocValuesProducer.getSortedNumeric(field);
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        return this.lucene90DocValuesProducer.getSortedSet(field);
    }

    @Override
    public void checkIntegrity() throws IOException {
        this.lucene90DocValuesProducer.checkIntegrity();
    }

    public SortedNumericDocValues getSortedNumeric(String fieldName) throws IOException {
        return this.lucene90DocValuesProducer.getSortedNumeric(fieldInfos.fieldInfo(fieldName));
    }

    public NumericDocValues getNumeric(String fieldName) throws IOException {
        return this.lucene90DocValuesProducer.getNumeric(fieldInfos.fieldInfo(fieldName));
    }

    @Override
    public void close() throws IOException {
        this.lucene90DocValuesProducer.close();
    }

    public FieldInfo[] getFieldInfoList() {
        List<String> metrics = new ArrayList<>();
        // TODO : remove this
        metrics.add("status_sum");
        // metrics.add("status_count");

        FieldInfo[] fieldInfoList = new FieldInfo[dimensionSplitOrder.size() + metrics.size()];
        int fieldNum = 0;
        for (String s : dimensionSplitOrder) {
            fieldInfoList[fieldNum] = new FieldInfo(
                s + "_dim",
                fieldNum,
                false,
                false,
                true,
                IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS,
                DocValuesType.SORTED_NUMERIC,
                -1,
                Collections.emptyMap(),
                0,
                0,
                0,
                0,
                VectorEncoding.FLOAT32,
                VectorSimilarityFunction.EUCLIDEAN,
                false,
                false
            );
            fieldNum++;
        }
        for (String metric : metrics) {
            fieldInfoList[fieldNum] = new FieldInfo(
                metric + "_metric",
                fieldNum,
                false,
                false,
                true,
                IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS,
                DocValuesType.SORTED_NUMERIC,
                -1,
                Collections.emptyMap(),
                0,
                0,
                0,
                0,
                VectorEncoding.FLOAT32,
                VectorSimilarityFunction.EUCLIDEAN,
                false,
                false
            );
            fieldNum++;
        }
        return fieldInfoList;
    }

}
