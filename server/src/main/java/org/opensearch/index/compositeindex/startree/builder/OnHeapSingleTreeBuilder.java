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
import org.apache.lucene.index.BaseSingleStarTreeBuilder;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.index.compositeindex.CompositeField;
import org.opensearch.index.compositeindex.startree.data.StarTreeDocValues;
import org.opensearch.index.compositeindex.startree.data.StarTreeDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * On heap single tree builder
 * This is not well tested - as initial benchmarks
 */
public class OnHeapSingleTreeBuilder extends BaseSingleStarTreeBuilder {

    private final List<StarTreeDocument> starTreeDocuments = new ArrayList<>();

    public OnHeapSingleTreeBuilder(
        CompositeField compositeField,
        DocValuesProducer docValuesProducer,
        DocValuesConsumer consumer,
        SegmentWriteState state
    ) throws IOException {
        super(compositeField, docValuesProducer, consumer, state);
    }

    @Override
    public void appendStarTreeDocument(StarTreeDocument starTreeDocument) throws IOException {
        starTreeDocuments.add(starTreeDocument);
    }

    @Override
    public StarTreeDocument getStarTreeDocument(int docId) throws IOException {
        return starTreeDocuments.get(docId);
    }

    @Override
    public List<StarTreeDocument> getStarTreeDocuments() throws IOException {
        return starTreeDocuments;
    }

    // TODO: should this be just long?
    @Override
    public long getDimensionValue(int docId, int dimensionId) throws IOException {
        return starTreeDocuments.get(docId).dimensions[dimensionId];
    }

    @Override
    public void build(List<StarTreeDocValues> starTreeDocValues) throws IOException {
        build(mergeStarTreeDocuments(starTreeDocValues));
    }

    Iterator<StarTreeDocument> mergeStarTreeDocuments(List<StarTreeDocValues> starTreeDocValues) throws IOException {
        // TODO: THIS DOES NOT SUPPORT KEYWORDS YET.
        List<StarTreeDocument> toBeMergedStarTreeDocuments = new ArrayList<>();
        for (StarTreeDocValues starTree : starTreeDocValues) {
            boolean endOfDoc = false;
            while (!endOfDoc) {
                long[] dims = new long[starTree.dimensionValues.size()];
                int i = 0;
                // are we breaking order here?
                for (Map.Entry<String, SortedNumericDocValues> dimValue : starTree.dimensionValues.entrySet()) {
                    int doc = dimValue.getValue().nextDoc();
                    long val = dimValue.getValue().nextValue();
                    if (doc == DocIdSetIterator.NO_MORE_DOCS || val == -1) {
                        endOfDoc = true;
                        break;
                    }
                    dims[i] = val;
                    i++;
                }
                if (endOfDoc) break;
                i = 0;
                Object[] metrics = new Object[starTree.metricValues.size()];
                for (Map.Entry<String, SortedNumericDocValues> metricValue : starTree.metricValues.entrySet()) {
                    metricValue.getValue().nextDoc();
                    metrics[i] = metricValue.getValue().nextValue();
                    i++;
                }
                StarTreeDocument toBeMergedStarTreeDocument = new StarTreeDocument(dims, metrics);
                toBeMergedStarTreeDocuments.add(toBeMergedStarTreeDocument);
            }
        }
        StarTreeDocument[] intermediateStarTreeDocuments = new StarTreeDocument[toBeMergedStarTreeDocuments.size()];
        toBeMergedStarTreeDocuments.toArray(intermediateStarTreeDocuments);
        return mergeStarTreeDocuments(intermediateStarTreeDocuments);
    }

    @Override
    public Iterator<StarTreeDocument> sortAndAggregateSegmentStarTreeDocument(int numDocs) throws IOException {
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[numDocs];
        for (int i = 0; i < numDocs; i++) {
            starTreeDocuments[i] = getNextSegmentStarTreeDocument();
        }
        return sortAndAggregateSegmentStarTreeDocuments(starTreeDocuments);
    }

    public Iterator<StarTreeDocument> sortAndAggregateSegmentStarTreeDocuments(StarTreeDocument[] starTreeDocuments) throws IOException {
        Arrays.sort(starTreeDocuments, (o1, o2) -> {
            for (int i = 0; i < numDimensions; i++) {
                if (o1.dimensions[i] != o2.dimensions[i]) {
                    return Math.toIntExact(o1.dimensions[i] - o2.dimensions[i]);
                }
            }
            return 0;
        });
        return mergeStarTreeDocuments(starTreeDocuments);
    }

    private Iterator<StarTreeDocument> mergeStarTreeDocuments(StarTreeDocument[] starTreeDocuments) {
        return new Iterator<>() {
            boolean _hasNext = true;
            StarTreeDocument currentStarTreeDocument = starTreeDocuments[0];
            int docId = 1;

            @Override
            public boolean hasNext() {
                return _hasNext;
            }

            @Override
            public StarTreeDocument next() {
                StarTreeDocument next = mergeStarTreeDocument(null, currentStarTreeDocument);
                while (docId < starTreeDocuments.length) {
                    StarTreeDocument starTreeDocument = starTreeDocuments[docId++];
                    if (!Arrays.equals(starTreeDocument.dimensions, next.dimensions)) {
                        currentStarTreeDocument = starTreeDocument;
                        return next;
                    } else {
                        next = mergeStarTreeDocument(next, starTreeDocument);
                    }
                }
                _hasNext = false;
                return next;
            }
        };
    }

    @Override
    public Iterator<StarTreeDocument> generateStarTreeForStarNode(int startDocId, int endDocId, int dimensionId) throws IOException {
        int numDocs = endDocId - startDocId;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[numDocs];
        for (int i = 0; i < numDocs; i++) {
            starTreeDocuments[i] = getStarTreeDocument(startDocId + i);
        }
        Arrays.sort(starTreeDocuments, (o1, o2) -> {
            for (int i = dimensionId + 1; i < numDimensions; i++) {
                if (o1.dimensions[i] != o2.dimensions[i]) {
                    return Math.toIntExact(o1.dimensions[i] - o2.dimensions[i]);
                }
            }
            return 0;
        });
        return new Iterator<StarTreeDocument>() {
            boolean _hasNext = true;
            StarTreeDocument currentStarTreeDocument = starTreeDocuments[0];
            int _docId = 1;

            private boolean hasSameDimensions(StarTreeDocument starTreeDocument1, StarTreeDocument starTreeDocument2) {
                for (int i = dimensionId + 1; i < numDimensions; i++) {
                    if (starTreeDocument1.dimensions[i] != starTreeDocument2.dimensions[i]) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public boolean hasNext() {
                return _hasNext;
            }

            @Override
            public StarTreeDocument next() {
                StarTreeDocument next = mergeStarTreeDocument(null, currentStarTreeDocument);
                next.dimensions[dimensionId] = STAR_IN_DOC_VALUES_INDEX;
                while (_docId < numDocs) {
                    StarTreeDocument starTreeDocument = starTreeDocuments[_docId++];
                    if (!hasSameDimensions(starTreeDocument, currentStarTreeDocument)) {
                        currentStarTreeDocument = starTreeDocument;
                        return next;
                    } else {
                        next = mergeStarTreeDocument(next, starTreeDocument);
                    }
                }
                _hasNext = false;
                return next;
            }
        };
    }
}
