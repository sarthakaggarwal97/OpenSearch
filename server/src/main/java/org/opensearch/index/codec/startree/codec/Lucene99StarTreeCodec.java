/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.codec.startree.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.codec.startree.builder.StarTreeConfig;
import org.opensearch.index.codec.startree.builder.StarTreeIndexConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * Codec to perform aggregation during indexing
 * Doc values format is extended to accommodate star tree index creation
 * */
public class Lucene99StarTreeCodec extends FilterCodec implements StarTreeConfig {
    private final DocValuesFormat dvFormat;

    /**
     * Creates a new Lucene95StarTreeCodec.
     */
    public Lucene99StarTreeCodec() {
        this(CodecService.STAR_TREE_CODEC, new Lucene99Codec());
    }

    /**
     * Sole constructor. When subclassing this codec, create a no-arg ctor and pass the delegate codec
     * and a unique name to this ctor.
     *
     * @param name
     * @param delegate
     */
    public Lucene99StarTreeCodec(String name, Codec delegate) {
        super(name, delegate);
        dvFormat = new StarTreeDocValuesFormat(this.starTreeIndexConfigs());
    }

    @Override
    public DocValuesFormat docValuesFormat() {
        return dvFormat;
    }

    @Override
    public List<StarTreeIndexConfig> starTreeIndexConfigs() {
        return new ArrayList<>();
    }
}
