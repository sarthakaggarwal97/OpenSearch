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
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

public class FilterDecompressor extends Decompressor {

    Decompressor decompressor;
    private static final Logger logger = LogManager.getLogger(FilterDecompressor.class);

    public FilterDecompressor(Decompressor decompressor){
        this.decompressor = decompressor;
    }

    @Override
    public void decompress(DataInput in, int originalLength, int offset, int length, BytesRef bytes) throws IOException {
        long startTime = System.nanoTime();
        decompressor.decompress(in, originalLength, offset, length, bytes);
        long endTime = System.nanoTime();
        logger.debug("Decompression Latency: " + (endTime - startTime));
    }

    @Override
    public Decompressor clone() {
        return new FilterDecompressor(decompressor);
    }
}
