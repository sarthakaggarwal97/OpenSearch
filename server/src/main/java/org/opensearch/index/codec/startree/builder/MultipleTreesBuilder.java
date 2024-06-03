/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.startree.builder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentWriteState;
import org.opensearch.index.codec.startree.codec.StarTreeAggregatedValues;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MultipleTreesBuilder implements Closeable {
    private static final Logger LOGGER = LogManager.getLogger(MultipleTreesBuilder.class);
    private final List<StarTreeIndexConfig> _builderConfigs;
    private final StarTreeBuildMode _buildMode;
    private final DocValuesConsumer docValuesConsumer;
    private final SegmentWriteState state;
    private final DocValuesProducer docValuesProducer;

    /**
     * Constructor for the multiple star-trees builder.
     *
     * @param builderConfigs List of builder configs (should already be deduplicated)
     * @param buildMode      Build mode (ON_HEAP or OFF_HEAP)
     */
    public MultipleTreesBuilder(
        List<StarTreeIndexConfig> builderConfigs,
        StarTreeBuildMode buildMode,
        DocValuesProducer docValuesProducer,
        DocValuesConsumer docValuesConsumer,
        SegmentWriteState state
    ) {

        if (builderConfigs == null || builderConfigs.isEmpty()) {
            throw new IllegalArgumentException("Must provide star-tree builder configs");
        }
        _builderConfigs = builderConfigs;
        _buildMode = buildMode;
        this.docValuesProducer = docValuesProducer;
        this.docValuesConsumer = docValuesConsumer;
        this.state = state;
    }

    /**
     * Builds the star-trees.
     */
    public void build() throws Exception {
        long startTime = System.currentTimeMillis();
        int numStarTrees = _builderConfigs.size();
        LOGGER.info("Starting building {} star-trees with configs: {} using {} builder", numStarTrees, _builderConfigs, _buildMode);

        // Build all star-trees
        for (int i = 0; i < numStarTrees; i++) {
            StarTreeIndexConfig builderConfig = _builderConfigs.get(i);
            try (
                SingleTreeBuilder singleTreeBuilder = getSingleTreeBuilder(
                    builderConfig,
                    _buildMode,
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
            _builderConfigs,
            _buildMode
        );
    }

    @Override
    public void close() throws IOException {

    }

    private static SingleTreeBuilder getSingleTreeBuilder(
        StarTreeIndexConfig builderConfig,
        StarTreeBuildMode buildMode,
        DocValuesProducer docValuesProducer,
        DocValuesConsumer docValuesConsumer,
        SegmentWriteState state
    ) throws IOException {
        if (buildMode == StarTreeBuildMode.ON_HEAP) {
            return new OnHeapSingleTreeBuilder(builderConfig, docValuesProducer, docValuesConsumer, state);
        } else {
            return new OffHeapSingleTreeBuilder(builderConfig, docValuesProducer, docValuesConsumer, state);
        }
    }

    public void build(Map<StarTreeIndexConfig, List<StarTreeAggregatedValues>> aggrMap) {
        for (Map.Entry<StarTreeIndexConfig, List<StarTreeAggregatedValues>> aggregateStarTreeEntry : aggrMap.entrySet()) {
            StarTreeIndexConfig config = aggregateStarTreeEntry.getKey();
            List<StarTreeAggregatedValues> values = aggregateStarTreeEntry.getValue();
            try (
                SingleTreeBuilder singleTreeBuilder = getSingleTreeBuilder(config, _buildMode, docValuesProducer, docValuesConsumer, state)
            ) {
                singleTreeBuilder.build(values);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
