/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.codec.startree.query;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.DocIdSetBuilder;
import org.opensearch.index.codec.startree.codec.StarTreeAggregatedValues;
import org.opensearch.index.codec.startree.node.StarTree;
import org.opensearch.index.codec.startree.node.StarTreeNode;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.Predicate;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/** Filter operator for star tree data structure. */
public class StarTreeFilter {
    private static final Logger logger = LogManager.getLogger(StarTreeFilter.class);

    /** Helper class to wrap the result from traversing the star tree. */
    static class StarTreeResult {
        final DocIdSetBuilder _matchedDocIds;
        final Set<String> _remainingPredicateColumns;
        final int numOfMatchedDocs;
        final int maxMatchedDoc;

        StarTreeResult(DocIdSetBuilder matchedDocIds, Set<String> remainingPredicateColumns, int numOfMatchedDocs, int maxMatchedDoc) {
            _matchedDocIds = matchedDocIds;
            _remainingPredicateColumns = remainingPredicateColumns;
            this.numOfMatchedDocs = numOfMatchedDocs;
            this.maxMatchedDoc = maxMatchedDoc;
        }
    }

    private final StarTree _starTree;

    Map<String, List<Predicate<Long>>> _predicateEvaluators;
    private final Set<String> _groupByColumns;

    DocIdSetBuilder docsWithField;

    DocIdSetBuilder.BulkAdder adder;
    Map<String, SortedNumericDocValues> dimValueMap;

    public StarTreeFilter(
        StarTreeAggregatedValues starTreeAggrStructure,
        Map<String, List<Predicate<Long>>> predicateEvaluators,
        Set<String> groupByColumns
    ) throws IOException {
        // This filter operator does not support AND/OR/NOT operations.
        _starTree = starTreeAggrStructure._starTree;
        dimValueMap = starTreeAggrStructure.dimensionValues;
        _predicateEvaluators = predicateEvaluators != null ? predicateEvaluators : Collections.emptyMap();
        _groupByColumns = groupByColumns != null ? groupByColumns : Collections.emptySet();

        // TODO : this should be the maximum number of doc values
        docsWithField = new DocIdSetBuilder(Integer.MAX_VALUE);
    }

    /**
     * <ul>
     *   <li>First go over the star tree and try to match as many dimensions as possible
     *   <li>For the remaining columns, use doc values indexes to match them
     * </ul>
     */
    public DocIdSetIterator getStarTreeResult() throws IOException {
        StarTreeResult starTreeResult = traverseStarTree();
        // logger.info("Matched docs in star tree : {}" , starTreeResult.numOfMatchedDocs);
        List<DocIdSetIterator> andIterators = new ArrayList<>();
        andIterators.add(starTreeResult._matchedDocIds.build().iterator());
        DocIdSetIterator docIdSetIterator = andIterators.get(0);
        // No matches, return
        if (starTreeResult.maxMatchedDoc == -1) {
            return docIdSetIterator;
        }
        int docCount = 0;
        for (String remainingPredicateColumn : starTreeResult._remainingPredicateColumns) {
            // TODO : set to max value of doc values
            logger.info("remainingPredicateColumn : {}, maxMatchedDoc : {} ", remainingPredicateColumn, starTreeResult.maxMatchedDoc);
            DocIdSetBuilder builder = new DocIdSetBuilder(starTreeResult.maxMatchedDoc + 1);
            List<Predicate<Long>> compositePredicateEvaluators = _predicateEvaluators.get(remainingPredicateColumn);
            SortedNumericDocValues ndv = this.dimValueMap.get(remainingPredicateColumn);
            List<Integer> docIds = new ArrayList<>();
            while (docIdSetIterator.nextDoc() != NO_MORE_DOCS) {
                docCount++;
                int docID = docIdSetIterator.docID();
                if (ndv.advanceExact(docID)) {
                    final int valuesCount = ndv.docValueCount();
                    long value = ndv.nextValue();
                    for (Predicate<Long> compositePredicateEvaluator : compositePredicateEvaluators) {
                        // TODO : this might be expensive as its done against all doc values docs
                        if (compositePredicateEvaluator.test(value)) {
                            docIds.add(docID);
                            for (int i = 0; i < valuesCount - 1; i++) {
                                while (docIdSetIterator.nextDoc() != NO_MORE_DOCS) {
                                    docIds.add(docIdSetIterator.docID());
                                }
                            }
                            break;
                        }
                    }
                }
            }
            DocIdSetBuilder.BulkAdder adder = builder.grow(docIds.size());
            for (int docID : docIds) {
                adder.add(docID);
            }
            docIdSetIterator = builder.build().iterator();
        }
        return docIdSetIterator;
    }

    /**
     * Helper method to traverse the star tree, get matching documents and keep track of all the
     * predicate dimensions that are not matched.
     */
    private StarTreeResult traverseStarTree() throws IOException {
        Set<String> globalRemainingPredicateColumns = null;

        StarTree starTree = _starTree;
        List<String> dimensionNames = starTree.getDimensionNames();
        StarTreeNode starTreeRootNode = starTree.getRoot();

        // Track whether we have found a leaf node added to the queue. If we have found a leaf node, and
        // traversed to the
        // level of the leave node, we can set globalRemainingPredicateColumns if not already set
        // because we know the leaf
        // node won't split further on other predicate columns.
        boolean foundLeafNode = starTreeRootNode.isLeaf();

        // Use BFS to traverse the star tree
        Queue<StarTreeNode> queue = new ArrayDeque<>();
        queue.add(starTreeRootNode);
        int currentDimensionId = -1;
        Set<String> remainingPredicateColumns = new HashSet<>(_predicateEvaluators.keySet());
        Set<String> remainingGroupByColumns = new HashSet<>(_groupByColumns);
        if (foundLeafNode) {
            globalRemainingPredicateColumns = new HashSet<>(remainingPredicateColumns);
        }

        int matchedDocsCountInStarTree = 0;
        int maxDocNum = -1;

        StarTreeNode starTreeNode;
        List<Integer> docIds = new ArrayList<>();
        while ((starTreeNode = queue.poll()) != null) {
            int dimensionId = starTreeNode.getDimensionId();
            if (dimensionId > currentDimensionId) {
                // Previous level finished
                String dimension = dimensionNames.get(dimensionId);
                remainingPredicateColumns.remove(dimension);
                remainingGroupByColumns.remove(dimension);
                if (foundLeafNode && globalRemainingPredicateColumns == null) {
                    globalRemainingPredicateColumns = new HashSet<>(remainingPredicateColumns);
                }
                currentDimensionId = dimensionId;
            }

            // If all predicate columns and group-by columns are matched, we can use aggregated document
            if (remainingPredicateColumns.isEmpty() && remainingGroupByColumns.isEmpty()) {
                int docId = starTreeNode.getAggregatedDocId();
                docIds.add(docId);
                matchedDocsCountInStarTree++;
                maxDocNum = docId > maxDocNum ? docId : maxDocNum;
                continue;
            }

            // For leaf node, because we haven't exhausted all predicate columns and group-by columns, we
            // cannot use
            // the aggregated document. Add the range of documents for this node to the bitmap, and keep
            // track of the
            // remaining predicate columns for this node
            if (starTreeNode.isLeaf()) {
                for (long i = starTreeNode.getStartDocId(); i < starTreeNode.getEndDocId(); i++) {
                    docIds.add((int) i);
                    matchedDocsCountInStarTree++;
                    maxDocNum = (int) i > maxDocNum ? (int) i : maxDocNum;
                }
                continue;
            }

            // For non-leaf node, proceed to next level
            String childDimension = dimensionNames.get(dimensionId + 1);

            // Only read star-node when the dimension is not in the global remaining predicate columns or
            // group-by columns
            // because we cannot use star-node in such cases
            StarTreeNode starNode = null;
            if ((globalRemainingPredicateColumns == null || !globalRemainingPredicateColumns.contains(childDimension))
                && !remainingGroupByColumns.contains(childDimension)) {
                starNode = starTreeNode.getChildForDimensionValue(StarTreeNode.ALL);
            }

            if (remainingPredicateColumns.contains(childDimension)) {
                // Have predicates on the next level, add matching nodes to the queue

                // Calculate the matching dictionary ids for the child dimension
                int numChildren = starTreeNode.getNumChildren();

                // If number of matching dictionary ids is large, use scan instead of binary search

                Iterator<? extends StarTreeNode> childrenIterator = starTreeNode.getChildrenIterator();

                // When the star-node exists, and the number of matching doc ids is more than or equal to
                // the
                // number of non-star child nodes, check if all the child nodes match the predicate, and use
                // the star-node if so
                if (starNode != null) {
                    List<StarTreeNode> matchingChildNodes = new ArrayList<>();
                    boolean findLeafChildNode = false;
                    while (childrenIterator.hasNext()) {
                        StarTreeNode childNode = childrenIterator.next();
                        List<Predicate<Long>> predicates = _predicateEvaluators.get(childDimension);
                        for (Predicate<Long> predicate : predicates) {
                            long val = childNode.getDimensionValue();
                            if (predicate.test(val)) {
                                matchingChildNodes.add(childNode);
                                findLeafChildNode |= childNode.isLeaf();
                                break;
                            }
                        }
                    }
                    if (matchingChildNodes.size() == numChildren - 1) {
                        // All the child nodes (except for the star-node) match the predicate, use the star-node
                        queue.add(starNode);
                        foundLeafNode |= starNode.isLeaf();
                    } else {
                        // Some child nodes do not match the predicate, use the matching child nodes
                        queue.addAll(matchingChildNodes);
                        foundLeafNode |= findLeafChildNode;
                    }
                } else {
                    // Cannot use the star-node, use the matching child nodes
                    while (childrenIterator.hasNext()) {
                        StarTreeNode childNode = childrenIterator.next();
                        List<Predicate<Long>> predicates = _predicateEvaluators.get(childDimension);
                        for (Predicate<Long> predicate : predicates) {
                            if (predicate.test(childNode.getDimensionValue())) {
                                queue.add(childNode);
                                foundLeafNode |= childNode.isLeaf();
                                break;
                            }
                        }
                    }
                }
            } else {
                // No predicate on the next level

                if (starNode != null) {
                    // Star-node exists, use it
                    queue.add(starNode);
                    foundLeafNode |= starNode.isLeaf();
                } else {
                    // Star-node does not exist or cannot be used, add all non-star nodes to the queue
                    Iterator<? extends StarTreeNode> childrenIterator = starTreeNode.getChildrenIterator();
                    while (childrenIterator.hasNext()) {
                        StarTreeNode childNode = childrenIterator.next();
                        if (childNode.getDimensionValue() != StarTreeNode.ALL) {
                            queue.add(childNode);
                            foundLeafNode |= childNode.isLeaf();
                        }
                    }
                }
            }
        }

        adder = docsWithField.grow(docIds.size());
        for (int id : docIds) {
            adder.add(id);
        }
        return new StarTreeResult(
            docsWithField,
            globalRemainingPredicateColumns != null ? globalRemainingPredicateColumns : Collections.emptySet(),
            matchedDocsCountInStarTree,
            maxDocNum
        );
    }
}
