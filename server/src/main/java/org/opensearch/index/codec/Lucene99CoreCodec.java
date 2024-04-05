/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.lucene95.Lucene95Codec;
import org.opensearch.index.mapper.MapperService;

/**
 *
 * Extends {@link FilterCodec} to reuse the functionality of Lucene Codec.
 * Supports two lucene modes BEST_SPEED and BEST_COMPRESSION.
 * Uses Lucene99 as the delegate codec
 *
 * @opensearch.internal
 */
public class Lucene99CoreCodec extends FilterCodec {

    private final StoredFieldsFormat storedFieldsFormat;

    public Lucene99CoreCodec() {
        super("Lucene99Core", new Lucene95Codec());
        storedFieldsFormat = new Lucene99CoreStoredFieldsFormat();
    }

    public Lucene99CoreCodec(Lucene95Codec.Mode mode, Integer lz4BlockSize, Integer zlibBlockSize, Integer noopCompressionSize, Boolean enableHybridCompression) {
        super("Lucene99Core", new Lucene95Codec(mode));
        storedFieldsFormat = new Lucene99CoreStoredFieldsFormat(mode, lz4BlockSize, zlibBlockSize, noopCompressionSize, enableHybridCompression);
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return storedFieldsFormat;
    }

    public Lucene99CoreCodec(Lucene95Codec.Mode mode, Integer lz4BlockSize, Integer zlibBlockSize, Integer noopCompressionSize, Boolean enableHybridCompression, MapperService mapperService, Logger logger) {
        super("Lucene99Core", new PerFieldMappingPostingFormatCodec(mode, mapperService, logger));
        this.storedFieldsFormat = new Lucene99CoreStoredFieldsFormat(mode, lz4BlockSize, zlibBlockSize, noopCompressionSize, enableHybridCompression);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
