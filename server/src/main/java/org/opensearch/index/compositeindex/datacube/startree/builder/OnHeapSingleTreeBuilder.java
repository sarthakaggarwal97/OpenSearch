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
import org.apache.lucene.index.BaseSingleStarTreeBuilder;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.data.StarTreeDocument;
import org.opensearch.index.compositeindex.datacube.startree.data.StarTreeValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * On heap single tree builder
 */
public class OnHeapSingleTreeBuilder extends BaseSingleStarTreeBuilder {

    private final List<StarTreeDocument> starTreeDocuments = new ArrayList<>();

    /**
     * Constructor for OnHeapSingleTreeBuilder
     *
     * @param starTreeField star-tree field
     * @param docValuesProducer document values producer
     * @param docValuesConsumer document values consumer
     * @param segmentWriteState segment write state
     * @throws IOException throws an exception we are unable to construct an onheap star-tree
     */
    public OnHeapSingleTreeBuilder(
        StarTreeField starTreeField,
        DocValuesProducer docValuesProducer,
        DocValuesConsumer docValuesConsumer,
        SegmentWriteState segmentWriteState
    ) throws IOException {
        super(starTreeField, docValuesProducer, docValuesConsumer, segmentWriteState);
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

    // Handles star-tree rebuilds during merges :)
    @Override
    public void build(List<StarTreeValues> starTreeValues) throws IOException {
        build(mergeStarTreeDocuments(starTreeValues));
    }

    /**
     * Merges star-tree documents.
     * @param starTreeValues star-tree document values
     * @return iterator for star-tree documents
     * @throws IOException
     */
    Iterator<StarTreeDocument> mergeStarTreeDocuments(List<StarTreeValues> starTreeValues) throws IOException {
        // TODO: THIS DOES NOT SUPPORT KEYWORDS YET.
        List<StarTreeDocument> toBeMergedStarTreeDocuments = new ArrayList<>();
        for (StarTreeValues starTreeDocValue : starTreeValues) {
            boolean hasMoreDocs = true;
            while (hasMoreDocs) {
                long[] dimensionValues = new long[starTreeDocValue.dimensionValues.size()];
                int dimensionValueCounter = 0;
                // are we breaking order here?
                for (Map.Entry<String, SortedNumericDocValues> dimension : starTreeDocValue.dimensionValues.entrySet()) {
                    // do we need to do some error handling?
                    int doc = dimension.getValue().nextDoc();
                    long val = dimension.getValue().nextValue();
                    if (doc == DocIdSetIterator.NO_MORE_DOCS || val == -1) {
                        hasMoreDocs = false;
                        break;
                    }
                    dimensionValues[dimensionValueCounter] = val;
                    dimensionValueCounter++;
                }
                if (!hasMoreDocs) break;
                int metricValueCounter = 0;
                Object[] metrics = new Object[starTreeDocValue.metricValues.size()];
                for (Map.Entry<String, SortedNumericDocValues> metricValue : starTreeDocValue.metricValues.entrySet()) {
                    // do we need to do some error handling?
                    metricValue.getValue().nextDoc();
                    metrics[metricValueCounter] = metricValue.getValue().nextValue();
                    metricValueCounter++;
                }
                StarTreeDocument toBeMergedStarTreeDocument = new StarTreeDocument(dimensionValues, metrics);
                toBeMergedStarTreeDocuments.add(toBeMergedStarTreeDocument);
            }
        }
        StarTreeDocument[] intermediateStarTreeDocuments = new StarTreeDocument[toBeMergedStarTreeDocuments.size()];
        toBeMergedStarTreeDocuments.toArray(intermediateStarTreeDocuments);
        return mergeStarTreeDocuments(intermediateStarTreeDocuments);
    }

    @Override
    public Iterator<StarTreeDocument> processSegmentStarTreeDocuments(int numDocs) throws IOException {
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[numDocs];
        for (int i = 0; i < numDocs; i++) {
            starTreeDocuments[i] = getNextSegmentStarTreeDocument();
        }
        return processStarTreeDocuments(starTreeDocuments);
    }

    /**
     * Sort, aggregates and merges the star-tree documents
     * @param starTreeDocuments star-tree documents
     * @return iterator for star-tree documents
     * @throws IOException throws when unable to sort, merge and aggregate star-tree documents
     */
    public Iterator<StarTreeDocument> processStarTreeDocuments(StarTreeDocument[] starTreeDocuments) throws IOException {

        // sort the documents
        Arrays.sort(starTreeDocuments, (o1, o2) -> {
            for (int i = 0; i < numDimensions; i++) {
                if (o1.dimensions[i] != o2.dimensions[i]) {
                    return Math.toIntExact(o1.dimensions[i] - o2.dimensions[i]);
                }
            }
            return 0;
        });

        // merge the documents
        return mergeStarTreeDocuments(starTreeDocuments);
    }

    /**
     * Merges the star-tree documents
     * @param starTreeDocuments star-tree documents
     * @return iterator to aggregate star-tree documents
     */
    private Iterator<StarTreeDocument> mergeStarTreeDocuments(StarTreeDocument[] starTreeDocuments) {
        return new Iterator<>() {
            boolean hasNext = true;
            StarTreeDocument currentStarTreeDocument = starTreeDocuments[0];
            int docId = 1;

            @Override
            public boolean hasNext() {
                return hasNext;
            }

            @Override
            public StarTreeDocument next() {
                // aggregate as we move on to the next doc
                StarTreeDocument next = aggregateStarTreeDocument(null, currentStarTreeDocument);
                while (docId < starTreeDocuments.length) {
                    StarTreeDocument starTreeDocument = starTreeDocuments[docId++];
                    if (!Arrays.equals(starTreeDocument.dimensions, next.dimensions)) {
                        currentStarTreeDocument = starTreeDocument;
                        return next;
                    } else {
                        next = aggregateStarTreeDocument(next, starTreeDocument);
                    }
                }
                hasNext = false;
                return next;
            }
        };
    }

    /**
     * Generates a star-tree for a given star-node
     * @param startDocId  Start document id in the star-tree
     * @param endDocId    End document id (exclusive) in the star-tree
     * @param dimensionId Dimension id of the star-node
     * @return iterator for star-tree documents of star-node
     * @throws IOException throws when unable to generate star-tree for star-node
     */
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
            boolean hasNext = true;
            StarTreeDocument currentStarTreeDocument = starTreeDocuments[0];
            int docId = 1;

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
                return hasNext;
            }

            @Override
            public StarTreeDocument next() {
                StarTreeDocument next = aggregateStarTreeDocument(null, currentStarTreeDocument);
                next.dimensions[dimensionId] = STAR_IN_DOC_VALUES_INDEX;
                while (docId < numDocs) {
                    StarTreeDocument starTreeDocument = starTreeDocuments[docId++];
                    if (!hasSameDimensions(starTreeDocument, currentStarTreeDocument)) {
                        currentStarTreeDocument = starTreeDocument;
                        return next;
                    } else {
                        next = aggregateStarTreeDocument(next, starTreeDocument);
                    }
                }
                hasNext = false;
                return next;
            }
        };
    }
}
