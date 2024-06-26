/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.builder;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;

import org.mockito.Mockito;

import static org.mockito.Mockito.when;

public class StarTreeValuesIteratorFactoryTests extends OpenSearchTestCase {

    private static StarTreeDocValuesIteratorAdapter factory;
    private static FieldInfo mockFieldInfo;

    @BeforeClass
    public static void setup() {
        factory = new StarTreeDocValuesIteratorAdapter();
        mockFieldInfo = new FieldInfo(
            "field",
            1,
            false,
            false,
            true,
            IndexOptions.NONE,
            DocValuesType.NONE,
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

    public void testCreateIterator_SortedSet() throws IOException {
        DocValuesProducer producer = Mockito.mock(DocValuesProducer.class);
        SortedSetDocValues iterator = Mockito.mock(SortedSetDocValues.class);
        when(producer.getSortedSet(mockFieldInfo)).thenReturn(iterator);
        DocIdSetIterator result = factory.getDocValuesIterator(DocValuesType.SORTED_SET, mockFieldInfo, producer);
        assertEquals(iterator.getClass(), result.getClass());
    }

    public void testCreateIterator_SortedNumeric() throws IOException {
        DocValuesProducer producer = Mockito.mock(DocValuesProducer.class);
        SortedNumericDocValues iterator = Mockito.mock(SortedNumericDocValues.class);
        when(producer.getSortedNumeric(mockFieldInfo)).thenReturn(iterator);
        DocIdSetIterator result = factory.getDocValuesIterator(DocValuesType.SORTED_NUMERIC, mockFieldInfo, producer);
        assertEquals(iterator.getClass(), result.getClass());
    }

    public void testCreateIterator_UnsupportedType() {
        DocValuesProducer producer = Mockito.mock(DocValuesProducer.class);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            factory.getDocValuesIterator(DocValuesType.BINARY, mockFieldInfo, producer);
        });
        assertEquals("Unsupported DocValuesType: BINARY", exception.getMessage());
    }

    public void testGetNextValue_SortedSet() throws IOException {
        SortedSetDocValues iterator = Mockito.mock(SortedSetDocValues.class);
        when(iterator.nextOrd()).thenReturn(42L);

        long result = factory.getNextOrd(iterator);
        assertEquals(42L, result);
    }

    public void testGetNextValue_SortedNumeric() throws IOException {
        SortedNumericDocValues iterator = Mockito.mock(SortedNumericDocValues.class);
        when(iterator.nextValue()).thenReturn(123L);

        long result = factory.getNextOrd(iterator);
        assertEquals(123L, result);
    }

    public void testGetNextValue_UnsupportedIterator() {
        DocIdSetIterator iterator = Mockito.mock(DocIdSetIterator.class);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> { factory.getNextOrd(iterator); });
        assertEquals("Unsupported Iterator: " + iterator.toString(), exception.getMessage());
    }

    public void testNextDoc() throws IOException {
        DocIdSetIterator iterator = Mockito.mock(DocIdSetIterator.class);
        when(iterator.nextDoc()).thenReturn(5);

        int result = factory.nextDoc(iterator);
        assertEquals(5, result);
    }
}
