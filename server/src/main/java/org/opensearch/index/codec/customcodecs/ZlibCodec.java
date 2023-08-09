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
public class ZlibCodec extends FilterCodec {

    private final StoredFieldsFormat storedFieldsFormat;

    public ZlibCodec() {
        super("Lucene95CustomCodec", new Lucene95Codec(Lucene95Codec.Mode.BEST_COMPRESSION));
        this.storedFieldsFormat = new Lucene95CustomStoredFieldsFormat(Lucene95CustomCodec.Mode.ZLIB);
    }

    public ZlibCodec(MapperService mapperService, Logger logger) {
        super("Lucene95CustomCodec", new PerFieldMappingPostingFormatCodec(Lucene95Codec.Mode.BEST_COMPRESSION, mapperService, logger));
        this.storedFieldsFormat = new Lucene95CustomStoredFieldsFormat(Lucene95CustomCodec.Mode.ZLIB);
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
