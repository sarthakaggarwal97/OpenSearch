/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.DataOutput;

import java.io.IOException;

public class FilterCompressor extends Compressor {

    Compressor compressor;
    private static final Logger logger = LogManager.getLogger(FilterCompressor.class);

    public FilterCompressor (Compressor compressor){
        this.compressor = compressor;
    }

    @Override
    public void compress(ByteBuffersDataInput buffersInput, DataOutput out) throws IOException {
        long startTime = System.nanoTime();
        compressor.compress(buffersInput, out);
        long endTime = System.nanoTime();
        logger.debug("Compression Latency: " + (endTime - startTime));
    }

    @Override
    public void close() throws IOException {

    }
}
