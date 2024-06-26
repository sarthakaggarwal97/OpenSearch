/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.builder;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.Version;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeDocument;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.mapper.ContentPath;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MappingLookup;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OnHeapSingleTreeBuilderTests extends OpenSearchTestCase {

    private OnHeapSingleTreeBuilder builder;
    private MapperService mapperService;
    private List<Dimension> dimensionsOrder;
    private List<String> fields = List.of(
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
    private List<Metric> metrics;
    private Directory directory;
    private FieldInfo[] fieldsInfo;
    private StarTreeField compositeField;
    private Map<String, DocValuesProducer> fieldProducerMap;
    private DocValuesConsumer docValuesConsumer;
    private SegmentWriteState writeState;

    @Before
    public void setup() throws IOException {
        dimensionsOrder = List.of(new Dimension("field1"), new Dimension("field3"), new Dimension("field5"), new Dimension("field8"));
        metrics = List.of(new Metric("field2", List.of(MetricStat.SUM)), new Metric("field4", List.of(MetricStat.SUM)));

        docValuesConsumer = mock(DocValuesConsumer.class);
        DocValuesProducer docValuesProducer = mock(DocValuesProducer.class);

        compositeField = new StarTreeField(
            "test",
            dimensionsOrder,
            metrics,
            new StarTreeFieldConfiguration(1, Set.of("field8"), StarTreeFieldConfiguration.StarTreeBuildMode.ON_HEAP)
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
        fieldProducerMap = new HashMap<>();
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
            fieldProducerMap.put(fields.get(i), docValuesProducer);
        }
        FieldInfos fieldInfos = new FieldInfos(fieldsInfo);
        writeState = new SegmentWriteState(InfoStream.getDefault(), segmentInfo.dir, segmentInfo, fieldInfos, null, newIOContext(random()));

        mapperService = mock(MapperService.class);
        DocumentMapper documentMapper = mock(DocumentMapper.class);
        when(mapperService.documentMapper()).thenReturn(documentMapper);
        Settings settings = Settings.builder().put(settings(org.opensearch.Version.CURRENT).build()).build();
        NumberFieldMapper numberFieldMapper1 = new NumberFieldMapper.Builder("field2", NumberFieldMapper.NumberType.DOUBLE, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper2 = new NumberFieldMapper.Builder("field4", NumberFieldMapper.NumberType.DOUBLE, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        MappingLookup fieldMappers = new MappingLookup(
            Set.of(numberFieldMapper1, numberFieldMapper2),
            Collections.emptyList(),
            Collections.emptyList(),
            0,
            null
        );
        when(documentMapper.mappers()).thenReturn(fieldMappers);
        builder = new OnHeapSingleTreeBuilder(compositeField, fieldProducerMap, docValuesConsumer, writeState, mapperService);
    }

    public void test_sortMergeAndAggregateStarTreeDocument() throws IOException {

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

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0]);
            long metric2 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[1]);
            segmentStarTreeDocuments[i] = new StarTreeDocument(starTreeDocuments[i].dimensions, new Long[] { metric1, metric2 });
        }

        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortMergeAndAggregateStarTreeDocument(
            segmentStarTreeDocuments
        );
        int numOfAggregatedDocuments = 0;
        while (segmentStarTreeDocumentIterator.hasNext() && expectedStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = segmentStarTreeDocumentIterator.next();
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

    public void test_sortMergeAndAggregateStarTreeDocument_nullMetric() throws IOException {

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(new long[] { 2, 4, 3, 4 }, new Double[] { 12.0, 10.0 });
        starTreeDocuments[1] = new StarTreeDocument(new long[] { 3, 4, 2, 1 }, new Double[] { 10.0, 6.0 });
        starTreeDocuments[2] = new StarTreeDocument(new long[] { 3, 4, 2, 1 }, new Double[] { 14.0, 12.0 });
        starTreeDocuments[3] = new StarTreeDocument(new long[] { 2, 4, 3, 4 }, new Double[] { 9.0, 4.0 });
        starTreeDocuments[4] = new StarTreeDocument(new long[] { 3, 4, 2, 1 }, new Double[] { 11.0, null });
        StarTreeDocument expectedStarTreeDocument = new StarTreeDocument(new long[] { 2, 4, 3, 4 }, new Double[] { 21.0, 14.0 });

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            Long metric1 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0]);
            Long metric2 = starTreeDocuments[i].metrics[1] != null
                ? NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[1])
                : null;
            segmentStarTreeDocuments[i] = new StarTreeDocument(starTreeDocuments[i].dimensions, new Object[] { metric1, metric2 });
        }

        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortMergeAndAggregateStarTreeDocument(
            segmentStarTreeDocuments
        );

        StarTreeDocument resultStarTreeDocument = segmentStarTreeDocumentIterator.next();
        assertEquals(expectedStarTreeDocument.dimensions[0], resultStarTreeDocument.dimensions[0]);
        assertEquals(expectedStarTreeDocument.dimensions[1], resultStarTreeDocument.dimensions[1]);
        assertEquals(expectedStarTreeDocument.dimensions[2], resultStarTreeDocument.dimensions[2]);
        assertEquals(expectedStarTreeDocument.dimensions[3], resultStarTreeDocument.dimensions[3]);
        assertEquals(expectedStarTreeDocument.metrics[0], resultStarTreeDocument.metrics[0]);
        assertEquals(expectedStarTreeDocument.metrics[1], resultStarTreeDocument.metrics[1]);

        assertThrows(
            "Null metric should have resulted in IllegalStateException",
            IllegalStateException.class,
            segmentStarTreeDocumentIterator::next
        );

    }

    public void test_sortMergeAndAggregateStarTreeDocument_longMaxAndLongMinDimensions() throws IOException {

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(new long[] { Long.MIN_VALUE, 4, 3, 4 }, new Double[] { 12.0, 10.0 });
        starTreeDocuments[1] = new StarTreeDocument(new long[] { 3, 4, 2, Long.MAX_VALUE }, new Double[] { 10.0, 6.0 });
        starTreeDocuments[2] = new StarTreeDocument(new long[] { 3, 4, 2, Long.MAX_VALUE }, new Double[] { 14.0, 12.0 });
        starTreeDocuments[3] = new StarTreeDocument(new long[] { Long.MIN_VALUE, 4, 3, 4 }, new Double[] { 9.0, 4.0 });
        starTreeDocuments[4] = new StarTreeDocument(new long[] { 3, 4, 2, Long.MAX_VALUE }, new Double[] { 11.0, 16.0 });

        List<StarTreeDocument> inorderStarTreeDocuments = List.of(
            new StarTreeDocument(new long[] { Long.MIN_VALUE, 4, 3, 4 }, new Double[] { 21.0, 14.0 }),
            new StarTreeDocument(new long[] { 3, 4, 2, Long.MAX_VALUE }, new Double[] { 35.0, 34.0 })
        );
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = inorderStarTreeDocuments.iterator();

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0]);
            long metric2 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[1]);
            segmentStarTreeDocuments[i] = new StarTreeDocument(starTreeDocuments[i].dimensions, new Long[] { metric1, metric2 });
        }

        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortMergeAndAggregateStarTreeDocument(
            segmentStarTreeDocuments
        );
        int numOfAggregatedDocuments = 0;
        while (segmentStarTreeDocumentIterator.hasNext() && expectedStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = segmentStarTreeDocumentIterator.next();
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

    public void test_build_DoubleMaxAndDoubleMinMetrics() throws IOException {

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(new long[] { 2, 4, 3, 4 }, new Double[] { Double.MAX_VALUE, 10.0 });
        starTreeDocuments[1] = new StarTreeDocument(new long[] { 3, 4, 2, 1 }, new Double[] { 10.0, 6.0 });
        starTreeDocuments[2] = new StarTreeDocument(new long[] { 3, 4, 2, 1 }, new Double[] { 14.0, Double.MIN_VALUE });
        starTreeDocuments[3] = new StarTreeDocument(new long[] { 2, 4, 3, 4 }, new Double[] { 9.0, 4.0 });
        starTreeDocuments[4] = new StarTreeDocument(new long[] { 3, 4, 2, 1 }, new Double[] { 11.0, 16.0 });

        List<StarTreeDocument> inorderStarTreeDocuments = List.of(
            new StarTreeDocument(new long[] { 2, 4, 3, 4 }, new Double[] { Double.MAX_VALUE + 9, 14.0 }),
            new StarTreeDocument(new long[] { 3, 4, 2, 1 }, new Double[] { 35.0, Double.MIN_VALUE + 22 })
        );
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = inorderStarTreeDocuments.iterator();

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0]);
            long metric2 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[1]);
            segmentStarTreeDocuments[i] = new StarTreeDocument(starTreeDocuments[i].dimensions, new Long[] { metric1, metric2 });
        }

        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortMergeAndAggregateStarTreeDocument(
            segmentStarTreeDocuments
        );
        int numOfAggregatedDocuments = 0;
        while (segmentStarTreeDocumentIterator.hasNext() && expectedStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = segmentStarTreeDocumentIterator.next();
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

    public void test_build_halfFloatMetrics() throws IOException {

        mapperService = mock(MapperService.class);
        DocumentMapper documentMapper = mock(DocumentMapper.class);
        when(mapperService.documentMapper()).thenReturn(documentMapper);
        Settings settings = Settings.builder().put(settings(org.opensearch.Version.CURRENT).build()).build();
        NumberFieldMapper numberFieldMapper1 = new NumberFieldMapper.Builder("field2", NumberFieldMapper.NumberType.HALF_FLOAT, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper2 = new NumberFieldMapper.Builder("field4", NumberFieldMapper.NumberType.HALF_FLOAT, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        MappingLookup fieldMappers = new MappingLookup(
            Set.of(numberFieldMapper1, numberFieldMapper2),
            Collections.emptyList(),
            Collections.emptyList(),
            0,
            null
        );
        when(documentMapper.mappers()).thenReturn(fieldMappers);
        builder = new OnHeapSingleTreeBuilder(compositeField, fieldProducerMap, docValuesConsumer, writeState, mapperService);

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(
            new long[] { 2, 4, 3, 4 },
            new HalfFloatPoint[] { new HalfFloatPoint("hf1", 12), new HalfFloatPoint("hf6", 10) }
        );
        starTreeDocuments[1] = new StarTreeDocument(
            new long[] { 3, 4, 2, 1 },
            new HalfFloatPoint[] { new HalfFloatPoint("hf2", 10), new HalfFloatPoint("hf7", 6) }
        );
        starTreeDocuments[2] = new StarTreeDocument(
            new long[] { 3, 4, 2, 1 },
            new HalfFloatPoint[] { new HalfFloatPoint("hf3", 14), new HalfFloatPoint("hf8", 12) }
        );
        starTreeDocuments[3] = new StarTreeDocument(
            new long[] { 2, 4, 3, 4 },
            new HalfFloatPoint[] { new HalfFloatPoint("hf4", 9), new HalfFloatPoint("hf9", 4) }
        );
        starTreeDocuments[4] = new StarTreeDocument(
            new long[] { 3, 4, 2, 1 },
            new HalfFloatPoint[] { new HalfFloatPoint("hf5", 11), new HalfFloatPoint("hf10", 16) }
        );

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = HalfFloatPoint.halfFloatToSortableShort(
                ((HalfFloatPoint) starTreeDocuments[i].metrics[0]).numericValue().floatValue()
            );
            long metric2 = HalfFloatPoint.halfFloatToSortableShort(
                ((HalfFloatPoint) starTreeDocuments[i].metrics[1]).numericValue().floatValue()
            );
            segmentStarTreeDocuments[i] = new StarTreeDocument(starTreeDocuments[i].dimensions, new Long[] { metric1, metric2 });
        }

        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortMergeAndAggregateStarTreeDocument(
            segmentStarTreeDocuments
        );
        builder.build(segmentStarTreeDocumentIterator);

        List<StarTreeDocument> resultStarTreeDocuments = builder.getStarTreeDocuments();
        assertEquals(8, resultStarTreeDocuments.size());

        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = getExpectedStarTreeDocumentIterator();
        assertStarTreeDocuments(resultStarTreeDocuments, expectedStarTreeDocumentIterator);
    }

    public void test_build_floatMetrics() throws IOException {

        mapperService = mock(MapperService.class);
        DocumentMapper documentMapper = mock(DocumentMapper.class);
        when(mapperService.documentMapper()).thenReturn(documentMapper);
        Settings settings = Settings.builder().put(settings(org.opensearch.Version.CURRENT).build()).build();
        NumberFieldMapper numberFieldMapper1 = new NumberFieldMapper.Builder("field2", NumberFieldMapper.NumberType.FLOAT, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper2 = new NumberFieldMapper.Builder("field4", NumberFieldMapper.NumberType.FLOAT, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        MappingLookup fieldMappers = new MappingLookup(
            Set.of(numberFieldMapper1, numberFieldMapper2),
            Collections.emptyList(),
            Collections.emptyList(),
            0,
            null
        );
        when(documentMapper.mappers()).thenReturn(fieldMappers);
        builder = new OnHeapSingleTreeBuilder(compositeField, fieldProducerMap, docValuesConsumer, writeState, mapperService);

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(new long[] { 2, 4, 3, 4 }, new Float[] { 12.0F, 10.0F });
        starTreeDocuments[1] = new StarTreeDocument(new long[] { 3, 4, 2, 1 }, new Float[] { 10.0F, 6.0F });
        starTreeDocuments[2] = new StarTreeDocument(new long[] { 3, 4, 2, 1 }, new Float[] { 14.0F, 12.0F });
        starTreeDocuments[3] = new StarTreeDocument(new long[] { 2, 4, 3, 4 }, new Float[] { 9.0F, 4.0F });
        starTreeDocuments[4] = new StarTreeDocument(new long[] { 3, 4, 2, 1 }, new Float[] { 11.0F, 16.0F });

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = NumericUtils.floatToSortableInt((Float) starTreeDocuments[i].metrics[0]);
            long metric2 = NumericUtils.floatToSortableInt((Float) starTreeDocuments[i].metrics[1]);
            segmentStarTreeDocuments[i] = new StarTreeDocument(starTreeDocuments[i].dimensions, new Long[] { metric1, metric2 });
        }

        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortMergeAndAggregateStarTreeDocument(
            segmentStarTreeDocuments
        );
        builder.build(segmentStarTreeDocumentIterator);

        List<StarTreeDocument> resultStarTreeDocuments = builder.getStarTreeDocuments();
        assertEquals(8, resultStarTreeDocuments.size());

        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = getExpectedStarTreeDocumentIterator();
        assertStarTreeDocuments(resultStarTreeDocuments, expectedStarTreeDocumentIterator);
    }

    public void test_build_longMetrics() throws IOException {

        mapperService = mock(MapperService.class);
        DocumentMapper documentMapper = mock(DocumentMapper.class);
        when(mapperService.documentMapper()).thenReturn(documentMapper);
        Settings settings = Settings.builder().put(settings(org.opensearch.Version.CURRENT).build()).build();
        NumberFieldMapper numberFieldMapper1 = new NumberFieldMapper.Builder("field2", NumberFieldMapper.NumberType.LONG, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper2 = new NumberFieldMapper.Builder("field4", NumberFieldMapper.NumberType.LONG, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        MappingLookup fieldMappers = new MappingLookup(
            Set.of(numberFieldMapper1, numberFieldMapper2),
            Collections.emptyList(),
            Collections.emptyList(),
            0,
            null
        );
        when(documentMapper.mappers()).thenReturn(fieldMappers);
        builder = new OnHeapSingleTreeBuilder(compositeField, fieldProducerMap, docValuesConsumer, writeState, mapperService);

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(new long[] { 2, 4, 3, 4 }, new Long[] { 12L, 10L });
        starTreeDocuments[1] = new StarTreeDocument(new long[] { 3, 4, 2, 1 }, new Long[] { 10L, 6L });
        starTreeDocuments[2] = new StarTreeDocument(new long[] { 3, 4, 2, 1 }, new Long[] { 14L, 12L });
        starTreeDocuments[3] = new StarTreeDocument(new long[] { 2, 4, 3, 4 }, new Long[] { 9L, 4L });
        starTreeDocuments[4] = new StarTreeDocument(new long[] { 3, 4, 2, 1 }, new Long[] { 11L, 16L });

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = (Long) starTreeDocuments[i].metrics[0];
            long metric2 = (Long) starTreeDocuments[i].metrics[1];
            segmentStarTreeDocuments[i] = new StarTreeDocument(starTreeDocuments[i].dimensions, new Long[] { metric1, metric2 });
        }

        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortMergeAndAggregateStarTreeDocument(
            segmentStarTreeDocuments
        );
        builder.build(segmentStarTreeDocumentIterator);

        List<StarTreeDocument> resultStarTreeDocuments = builder.getStarTreeDocuments();
        assertEquals(8, resultStarTreeDocuments.size());

        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = getExpectedStarTreeDocumentIterator();
        assertStarTreeDocuments(resultStarTreeDocuments, expectedStarTreeDocumentIterator);
    }

    private static Iterator<StarTreeDocument> getExpectedStarTreeDocumentIterator() {
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
        return expectedStarTreeDocuments.iterator();
    }

    public void test_build() throws IOException {

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(new long[] { 2, 4, 3, 4 }, new Double[] { 12.0, 10.0 });
        starTreeDocuments[1] = new StarTreeDocument(new long[] { 3, 4, 2, 1 }, new Double[] { 10.0, 6.0 });
        starTreeDocuments[2] = new StarTreeDocument(new long[] { 3, 4, 2, 1 }, new Double[] { 14.0, 12.0 });
        starTreeDocuments[3] = new StarTreeDocument(new long[] { 2, 4, 3, 4 }, new Double[] { 9.0, 4.0 });
        starTreeDocuments[4] = new StarTreeDocument(new long[] { 3, 4, 2, 1 }, new Double[] { 11.0, 16.0 });

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0]);
            long metric2 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[1]);
            segmentStarTreeDocuments[i] = new StarTreeDocument(starTreeDocuments[i].dimensions, new Long[] { metric1, metric2 });
        }

        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortMergeAndAggregateStarTreeDocument(
            segmentStarTreeDocuments
        );
        builder.build(segmentStarTreeDocumentIterator);

        List<StarTreeDocument> resultStarTreeDocuments = builder.getStarTreeDocuments();
        assertEquals(8, resultStarTreeDocuments.size());

        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = getExpectedStarTreeDocumentIterator();
        assertStarTreeDocuments(resultStarTreeDocuments, expectedStarTreeDocumentIterator);
    }

    private static void assertStarTreeDocuments(
        List<StarTreeDocument> resultStarTreeDocuments,
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator
    ) {
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
