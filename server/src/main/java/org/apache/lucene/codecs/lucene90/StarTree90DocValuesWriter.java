/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.apache.lucene.codecs.lucene90;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.opensearch.index.codec.StarTreeReader;
import org.opensearch.index.codec.startree.builder.MultipleTreesBuilder;
import org.opensearch.index.codec.startree.builder.StarTreeBuildMode;
import org.opensearch.index.codec.startree.builder.StarTreeIndexConfig;
import org.opensearch.index.codec.startree.codec.StarTreeAggregatedValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.index.codec.startree.builder.StarTreeIndexConfig.getDimensionName;
import static org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat.DATA_CODEC;
import static org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat.META_CODEC;

/**
 * Custom star tree doc values writer
 */
public class StarTree90DocValuesWriter extends DocValuesConsumer {

    private final DocValuesConsumer delegate;

    // TODO : should we make all of this final ?

    private final List<StarTreeIndexConfig> starTreeIndexConfigs;

    List<String> dimensionsSplitOrder;

    MultipleTreesBuilder builder;

    DocValuesConsumer docValuesConsumer;
    DocValuesProducer docValuesProducer;
    private static final Logger logger = LogManager.getLogger(StarTree90DocValuesWriter.class);

    Set<String> starTreeFieldsSet = new HashSet<>();

    private boolean isMerge = false;

    public StarTree90DocValuesWriter(
        DocValuesConsumer delegate,
        SegmentWriteState segmentWriteState,
        List<StarTreeIndexConfig> starTreeIndexConfigs,
        StarTreeBuildMode buildMode
    ) throws Exception {
        this.delegate = delegate;
        this.starTreeIndexConfigs = starTreeIndexConfigs;
        dimensionsSplitOrder = new ArrayList<>();
        docValuesConsumer = new Lucene90DocValuesConsumer(segmentWriteState, DATA_CODEC, "sttd", META_CODEC, "sttm");
        this.builder = new MultipleTreesBuilder(starTreeIndexConfigs, buildMode, docValuesProducer, docValuesConsumer, segmentWriteState);
        populateStarTreeFieldsSet();
    }

    private void populateStarTreeFieldsSet() {
        for (StarTreeIndexConfig starTreeIndexConfig : starTreeIndexConfigs) {
            starTreeFieldsSet.addAll(getDimensionName(starTreeIndexConfig.getDimensionsSplitOrder()));
            starTreeFieldsSet.addAll(getDimensionName(starTreeIndexConfig.getAggregationFunctionColumnPairs().keySet()));
        }
    }

    @Override
    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addNumericField(field, valuesProducer);
        this.docValuesProducer = valuesProducer;
    }

    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addBinaryField(field, valuesProducer);
        this.docValuesProducer = valuesProducer;
    }

    @Override
    public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addSortedField(field, valuesProducer);
        this.docValuesProducer = valuesProducer;
    }

    @Override
    public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addSortedNumericField(field, valuesProducer);
        this.docValuesProducer = valuesProducer;
        starTreeFieldsSet.remove(field.name);
        if (starTreeFieldsSet.isEmpty()) {
            try {
                createStarTree();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addSortedSetField(field, valuesProducer);
        this.docValuesProducer = valuesProducer;
    }

    @Override
    public void merge(MergeState mergeState) throws IOException {
        // TODO : check if class variable will cause concurrency issues
        isMerge = true;
        super.merge(mergeState);
        isMerge = false;
        mergeAggregatedValues(mergeState);
    }

    public void mergeAggregatedValues(MergeState mergeState) throws IOException {
        Map<StarTreeIndexConfig, List<StarTreeAggregatedValues>> aggrMap = new HashMap<>();

        for (StarTreeIndexConfig starTreeIndexConfig : starTreeIndexConfigs) {
            List<StarTreeAggregatedValues> starTreeAggregatedValuesList = new ArrayList<>();
            for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
                StarTreeReader producer = (StarTreeReader) mergeState.docValuesProducers[i];
                StarTreeAggregatedValues starTree = producer.getStarTreeValues();
                starTreeAggregatedValuesList.add(starTree);
            }
            aggrMap.put(starTreeIndexConfig, starTreeAggregatedValuesList);
        }

        logger.info("Merging star-tree");
        long startTime = System.currentTimeMillis();
        builder.build(aggrMap);
        logger.info("Finished merging star-tree in ms : {}", (System.currentTimeMillis() - startTime));
    }

    public void createStarTree() throws Exception {
        if (isMerge) return;
        long startTime = System.currentTimeMillis();
        builder.build();
        logger.info("Finished building star-tree in ms : {}", (System.currentTimeMillis() - startTime));
    }

    @Override
    public void close() throws IOException {
        if (delegate != null) {
            delegate.close();
        }
        if (docValuesConsumer != null) {
            docValuesConsumer.close();
        }
        if (builder != null) {
            builder.close();
        }
    }
}
