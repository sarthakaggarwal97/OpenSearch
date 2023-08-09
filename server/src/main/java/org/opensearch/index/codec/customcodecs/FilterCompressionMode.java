/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;

public class FilterCompressionMode extends CompressionMode {

    CompressionMode compressionMode;

    public FilterCompressionMode(CompressionMode compressionMode) {
        this.compressionMode = compressionMode;
    }

    @Override
    public Compressor newCompressor() {
        return new FilterCompressor(compressionMode.newCompressor());
    }

    @Override
    public Decompressor newDecompressor() {
        return new FilterDecompressor(compressionMode.newDecompressor());
    }
}
