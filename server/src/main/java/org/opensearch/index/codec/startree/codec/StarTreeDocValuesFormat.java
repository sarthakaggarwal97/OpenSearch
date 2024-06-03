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

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.codecs.lucene90.StarTree90DocValuesReader;
import org.apache.lucene.codecs.lucene90.StarTree90DocValuesWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.opensearch.index.codec.startree.builder.StarTreeBuildMode;
import org.opensearch.index.codec.startree.builder.StarTreeIndexConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Custom doc values format for star tree codec */
public class StarTreeDocValuesFormat extends DocValuesFormat {
    /**
     * Creates a new docvalues format.
     *
     * <p>The provided name will be written into the index segment in some configurations (such as
     * when using {@code PerFieldDocValuesFormat}): in such configurations, for the segment to be read
     * this class should be registered with Java's SPI mechanism (registered in META-INF/ of your jar
     * file, etc).
     */

    List<StarTreeIndexConfig> starTreeIndexConfigs;
    StarTreeBuildMode starTreeBuildMode;

    private final DocValuesFormat delegate;

    public StarTreeDocValuesFormat() {
        this(new Lucene90DocValuesFormat(), new ArrayList<StarTreeIndexConfig>(), StarTreeBuildMode.OFF_HEAP);
    }

    public StarTreeDocValuesFormat(List<StarTreeIndexConfig> starTreeIndexConfigs) {
        this(new Lucene90DocValuesFormat(), starTreeIndexConfigs, StarTreeBuildMode.OFF_HEAP);
    }

    public StarTreeDocValuesFormat(
        DocValuesFormat delegate,
        List<StarTreeIndexConfig> starTreeIndexConfigs,
        StarTreeBuildMode starTreeBuildMode
    ) {
        super(delegate.getName());
        this.delegate = delegate;
        this.starTreeIndexConfigs = starTreeIndexConfigs;
        this.starTreeBuildMode = starTreeBuildMode;
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        try {
            return new StarTree90DocValuesWriter(delegate.fieldsConsumer(state), state, this.starTreeIndexConfigs, this.starTreeBuildMode);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new StarTree90DocValuesReader(delegate.fieldsProducer(state), state, this.starTreeIndexConfigs);
    }
}
