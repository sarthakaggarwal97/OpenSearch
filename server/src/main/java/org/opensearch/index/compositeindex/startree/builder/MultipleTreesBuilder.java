/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.startree.builder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentWriteState;
import org.opensearch.index.compositeindex.CompositeField;
import org.opensearch.index.compositeindex.CompositeIndexConfig;
import org.opensearch.index.compositeindex.StarTreeFieldSpec;
import org.opensearch.index.compositeindex.startree.data.StarTreeDocValues;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class MultipleTreesBuilder implements Closeable {
    private static final Logger LOGGER = LogManager.getLogger(MultipleTreesBuilder.class);
    private final List<CompositeField> compositeFieldConfigs;
    private final CompositeIndexConfig compositeIndexConfig;
    private final StarTreeFieldSpec.StarTreeBuildMode buildMode;
    private final DocValuesConsumer docValuesConsumer;
    private final SegmentWriteState state;
    private final DocValuesProducer docValuesProducer;

    public MultipleTreesBuilder(
        CompositeIndexConfig compositeIndexConfig,
        StarTreeFieldSpec.StarTreeBuildMode buildMode,
        DocValuesProducer docValuesProducer,
        DocValuesConsumer docValuesConsumer,
        SegmentWriteState state
    ) {
        this.compositeIndexConfig = compositeIndexConfig;
        this.compositeFieldConfigs = compositeIndexConfig.getCompositeFields();
        if (compositeFieldConfigs == null || compositeFieldConfigs.isEmpty()) {
            throw new IllegalArgumentException("Must provide star-tree builder configs");
        }
        this.buildMode = buildMode;
        this.docValuesProducer = docValuesProducer;
        this.docValuesConsumer = docValuesConsumer;
        this.state = state;
    }

    /**
     * Builds the star-trees.
     */
    public void build() throws Exception {
        long startTime = System.currentTimeMillis();
        int numStarTrees = compositeFieldConfigs.size();
        LOGGER.info("Starting building {} star-trees with configs: {} using {} builder", numStarTrees, compositeFieldConfigs, buildMode);

        // Build all star-trees
        for (int i = 0; i < numStarTrees; i++) {
            CompositeField compositeField = compositeFieldConfigs.get(i);
            try (
                SingleTreeBuilder singleTreeBuilder = getSingleTreeBuilder(
                    compositeField,
                    buildMode,
                    docValuesProducer,
                    docValuesConsumer,
                    state
                )
            ) {
                singleTreeBuilder.build();
            }
        }
        LOGGER.info(
            "Took {} ms to building {} star-trees with configs: {} using {} builder",
            System.currentTimeMillis() - startTime,
            numStarTrees,
            compositeFieldConfigs,
            buildMode
        );
    }

    @Override
    public void close() throws IOException {

    }

    private static SingleTreeBuilder getSingleTreeBuilder(
        CompositeField compositeField,
        StarTreeFieldSpec.StarTreeBuildMode buildMode,
        DocValuesProducer docValuesProducer,
        DocValuesConsumer docValuesConsumer,
        SegmentWriteState state
    ) throws IOException {
        if (buildMode == StarTreeFieldSpec.StarTreeBuildMode.ON_HEAP) {
            return new OnHeapSingleTreeBuilder(compositeField, docValuesProducer, docValuesConsumer, state);
        }
        throw new IllegalArgumentException(
            String.format(Locale.ROOT, "No star tree implementation is available for [%s] build mode", buildMode)
        );
    }

    public void build(Map<CompositeField, List<StarTreeDocValues>> aggrMap) {
        for (Map.Entry<CompositeField, List<StarTreeDocValues>> aggregateStarTreeEntry : aggrMap.entrySet()) {
            CompositeField compositeField = aggregateStarTreeEntry.getKey();
            List<StarTreeDocValues> values = aggregateStarTreeEntry.getValue();
            try (
                SingleTreeBuilder singleTreeBuilder = getSingleTreeBuilder(
                    compositeField,
                    buildMode,
                    docValuesProducer,
                    docValuesConsumer,
                    state
                )
            ) {
                singleTreeBuilder.build(values);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
