/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.startree.builder;

import org.opensearch.index.codec.startree.codec.StarTreeAggregatedValues;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * A star-tree builder that builds a single star-tree.
 */
public interface SingleTreeBuilder extends Closeable {

    /**
     * Builds the star-tree.
     */

    void build() throws Exception;

    void build(List<StarTreeAggregatedValues> aggrList) throws IOException;

}
