/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CodecServiceFactory;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.Plugin;

import java.util.Optional;

/**
 * A plugin that implements custom codecs. Supports these codecs:
 * <ul>
 * <li>ZSTD
 * <li>ZSTDNODICT
 * </ul>
 *
 * @opensearch.internal
 */
public final class CustomCodecPlugin extends Plugin implements EnginePlugin {

    /**
     * Creates a new instance
     */
    public CustomCodecPlugin() {}

    /**
     * @param indexSettings is the default indexSettings
     * @return the engine factory
     */
    @Override
    public Optional<CodecServiceFactory> getCustomCodecServiceFactory(final IndexSettings indexSettings) {
        String codecName = indexSettings.getValue(EngineConfig.INDEX_CODEC_SETTING);
        if (codecName.equals(CustomCodecService.ZSTD_NO_DICT_CODEC) || codecName.equals(CustomCodecService.ZSTD_CODEC)) {
            return Optional.of(new CustomCodecServiceFactory());
        }
        return Optional.empty();
    }
}
