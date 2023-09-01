/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.apache.logging.log4j.Logger;
import org.opensearch.index.mapper.MapperService;

/**
 * ZstdNoDictCodec provides ZSTD compressor without a dictionary support.
 */
public class ZstdNoDictCodec extends Lucene95CustomCodec {

    /**
     * Creates a new ZstdNoDictCodec instance with the default compression level.
     */
    public ZstdNoDictCodec() {
        this(DEFAULT_COMPRESSION_LEVEL);
    }

    /**
     * Creates a new ZstdNoDictCodec instance.
     *
     * @param compressionLevel The compression level.
     */
    public ZstdNoDictCodec(int compressionLevel) {
        super(Mode.ZSTD_NO_DICT, compressionLevel);
    }

    /**
     * Creates a new ZstdNoDictCodec instance.
     *
     * @param mapperService The mapper service.
     * @param logger The logger.
     * @param compressionLevel The compression level.
     */
    public ZstdNoDictCodec(MapperService mapperService, Logger logger, int compressionLevel) {
        super(Mode.ZSTD_NO_DICT, compressionLevel, mapperService, logger);
    }

    /** The name for this codec. */
    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
