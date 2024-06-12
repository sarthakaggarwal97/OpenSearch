/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.startree.builder;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.Version;
import org.opensearch.index.compositeindex.CompositeField;
import org.opensearch.index.compositeindex.Dimension;
import org.opensearch.index.compositeindex.Metric;
import org.opensearch.index.compositeindex.MetricType;
import org.opensearch.index.compositeindex.StarTreeFieldSpec;
import org.opensearch.index.compositeindex.startree.data.StarTreeDocument;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.mockito.Mockito.mock;

public class OnHeapSingleTreeBuilderTests extends OpenSearchTestCase {

    private static OnHeapSingleTreeBuilder builder;
    private static Lucene90DocValuesFormat lucene90DocValuesFormat;
    private static List<Dimension> dimensionsOrder;
    private static List<String> fields = List.of(
        "field1",
        "field2",
        "field3",
        "field4",
        "field5",
        "field6",
        "field7",
        "field8",
        "field9",
        "field10"
    );
    private static List<Metric> metrics;
    private static Directory directory;
    private static FieldInfo[] fieldsInfo;

    @BeforeClass
    public static void setup() throws IOException {
        dimensionsOrder = List.of(new Dimension("field1"), new Dimension("field3"), new Dimension("field5"), new Dimension("field8"));
        metrics = List.of(new Metric("field2", List.of(MetricType.SUM)), new Metric("field4", List.of(MetricType.SUM)));

        DocValuesConsumer docValuesConsumer = mock(DocValuesConsumer.class);
        DocValuesProducer docValuesProducer = mock(DocValuesProducer.class);

        CompositeField compositeField = new CompositeField(
            "test",
            dimensionsOrder,
            metrics,
            new StarTreeFieldSpec(1, Set.of("field8"), StarTreeFieldSpec.StarTreeBuildMode.ON_HEAP)
        );
        directory = newFSDirectory(createTempDir());
        SegmentInfo segmentInfo = new SegmentInfo(
            directory,
            Version.LATEST,
            Version.LUCENE_9_11_0,
            "test_segment",
            5,
            false,
            false,
            new Lucene99Codec(),
            new HashMap<>(),
            UUID.randomUUID().toString().substring(0, 16).getBytes(StandardCharsets.UTF_8),
            new HashMap<>(),
            null
        );

        fieldsInfo = new FieldInfo[fields.size()];

        for (int i = 0; i < fieldsInfo.length; i++) {
            fieldsInfo[i] = new FieldInfo(
                fields.get(i),
                i,
                false,
                false,
                true,
                IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS,
                DocValuesType.SORTED_NUMERIC,
                -1,
                Collections.emptyMap(),
                0,
                0,
                0,
                0,
                VectorEncoding.FLOAT32,
                VectorSimilarityFunction.EUCLIDEAN,
                false,
                false
            );
        }
        FieldInfos fieldInfos = new FieldInfos(fieldsInfo);
        final SegmentWriteState writeState = new SegmentWriteState(
            InfoStream.getDefault(),
            segmentInfo.dir,
            segmentInfo,
            fieldInfos,
            null,
            newIOContext(random())
        );

        builder = new OnHeapSingleTreeBuilder(compositeField, docValuesProducer, docValuesConsumer, writeState);
    }

    public void test_processStarTreeDocuments() throws IOException {

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(new long[] { 2, 4, 3, 4 }, new Double[] { 12.0, 10.0 });
        starTreeDocuments[1] = new StarTreeDocument(new long[] { 3, 4, 2, 1 }, new Double[] { 10.0, 6.0 });
        starTreeDocuments[2] = new StarTreeDocument(new long[] { 3, 4, 2, 1 }, new Double[] { 14.0, 12.0 });
        starTreeDocuments[3] = new StarTreeDocument(new long[] { 2, 4, 3, 4 }, new Double[] { 9.0, 4.0 });
        starTreeDocuments[4] = new StarTreeDocument(new long[] { 3, 4, 2, 1 }, new Double[] { 11.0, 16.0 });

        List<StarTreeDocument> inorderStarTreeDocuments = List.of(
            new StarTreeDocument(new long[] { 2, 4, 3, 4 }, new Double[] { 21.0, 14.0 }),
            new StarTreeDocument(new long[] { 3, 4, 2, 1 }, new Double[] { 35.0, 34.0 })
        );
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = inorderStarTreeDocuments.iterator();
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.processStarTreeDocuments(starTreeDocuments);
        int numOfAggregatedDocuments = 0;
        while (starTreeDocumentIterator.hasNext() && expectedStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = starTreeDocumentIterator.next();
            StarTreeDocument expectedStarTreeDocument = expectedStarTreeDocumentIterator.next();

            assertEquals(expectedStarTreeDocument.dimensions[0], resultStarTreeDocument.dimensions[0]);
            assertEquals(expectedStarTreeDocument.dimensions[1], resultStarTreeDocument.dimensions[1]);
            assertEquals(expectedStarTreeDocument.dimensions[2], resultStarTreeDocument.dimensions[2]);
            assertEquals(expectedStarTreeDocument.dimensions[3], resultStarTreeDocument.dimensions[3]);
            assertEquals(expectedStarTreeDocument.metrics[0], resultStarTreeDocument.metrics[0]);
            assertEquals(expectedStarTreeDocument.metrics[1], resultStarTreeDocument.metrics[1]);

            numOfAggregatedDocuments++;
        }

        assertEquals(inorderStarTreeDocuments.size(), numOfAggregatedDocuments);

    }

    public void test_build() throws IOException {

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(new long[] { 2, 4, 3, 4 }, new Double[] { 12.0, 10.0 });
        starTreeDocuments[1] = new StarTreeDocument(new long[] { 3, 4, 2, 1 }, new Double[] { 10.0, 6.0 });
        starTreeDocuments[2] = new StarTreeDocument(new long[] { 3, 4, 2, 1 }, new Double[] { 14.0, 12.0 });
        starTreeDocuments[3] = new StarTreeDocument(new long[] { 2, 4, 3, 4 }, new Double[] { 9.0, 4.0 });
        starTreeDocuments[4] = new StarTreeDocument(new long[] { 3, 4, 2, 1 }, new Double[] { 11.0, 16.0 });

        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.processStarTreeDocuments(starTreeDocuments);
        builder.build(starTreeDocumentIterator);

        List<StarTreeDocument> resultStarTreeDocuments = builder.getStarTreeDocuments();
        assertEquals(8, resultStarTreeDocuments.size());

        List<StarTreeDocument> expectedStarTreeDocuments = List.of(
            new StarTreeDocument(new long[] { 2, 4, 3, 4 }, new Double[] { 21.0, 14.0 }),
            new StarTreeDocument(new long[] { 3, 4, 2, 1 }, new Double[] { 35.0, 34.0 }),
            new StarTreeDocument(new long[] { -1, 4, 2, 1 }, new Double[] { 35.0, 34.0 }),
            new StarTreeDocument(new long[] { -1, 4, 3, 4 }, new Double[] { 21.0, 14.0 }),
            new StarTreeDocument(new long[] { -1, 4, -1, 1 }, new Double[] { 35.0, 34.0 }),
            new StarTreeDocument(new long[] { -1, 4, -1, 4 }, new Double[] { 21.0, 14.0 }),
            new StarTreeDocument(new long[] { -1, 4, -1, -1 }, new Double[] { 56.0, 48.0 }),
            new StarTreeDocument(new long[] { -1, -1, -1, -1 }, new Double[] { 56.0, 48.0 })
        );
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = expectedStarTreeDocuments.iterator();
        Iterator<StarTreeDocument> resultStarTreeDocumentIterator = resultStarTreeDocuments.iterator();
        while (resultStarTreeDocumentIterator.hasNext() && expectedStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = resultStarTreeDocumentIterator.next();
            StarTreeDocument expectedStarTreeDocument = expectedStarTreeDocumentIterator.next();

            assertEquals(expectedStarTreeDocument.dimensions[0], resultStarTreeDocument.dimensions[0]);
            assertEquals(expectedStarTreeDocument.dimensions[1], resultStarTreeDocument.dimensions[1]);
            assertEquals(expectedStarTreeDocument.dimensions[2], resultStarTreeDocument.dimensions[2]);
            assertEquals(expectedStarTreeDocument.dimensions[3], resultStarTreeDocument.dimensions[3]);
            assertEquals(expectedStarTreeDocument.metrics[0], resultStarTreeDocument.metrics[0]);
            assertEquals(expectedStarTreeDocument.metrics[1], resultStarTreeDocument.metrics[1]);
        }
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        directory.close();
    }
}
