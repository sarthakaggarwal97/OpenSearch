/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.lucene95.Lucene95Codec;
import org.opensearch.index.codec.PerFieldMappingPostingFormatCodec;
import org.opensearch.index.mapper.MapperService;

/**
 * ZstdCodec provides ZSTD compressor using the <a href="https://github.com/luben/zstd-jni">zstd-jni</a> library.
 */
public class LZ4Codec extends FilterCodec {

    private final StoredFieldsFormat storedFieldsFormat;

    public LZ4Codec() {
        super("Lucene95CustomCodec", new Lucene95Codec());
        this.storedFieldsFormat = new Lucene95CustomStoredFieldsFormat(Lucene95CustomCodec.Mode.LZ4);
    }

    public LZ4Codec( MapperService mapperService, Logger logger) {
        super("Lucene95CustomCodec", new PerFieldMappingPostingFormatCodec(Lucene95Codec.Mode.BEST_SPEED, mapperService, logger));
        this.storedFieldsFormat = new Lucene95CustomStoredFieldsFormat(Lucene95CustomCodec.Mode.LZ4);
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return storedFieldsFormat;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
