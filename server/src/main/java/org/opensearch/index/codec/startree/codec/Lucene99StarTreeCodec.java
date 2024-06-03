/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
