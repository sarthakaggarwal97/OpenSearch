/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.apache.lucene.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IOUtils;
import org.opensearch.common.time.DateUtils;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.codec.startree.aggregator.AggregationFunctionColumnPair;
import org.opensearch.index.codec.startree.aggregator.AggregationFunctionType;
import org.opensearch.index.codec.startree.aggregator.ValueAggregator;
import org.opensearch.index.codec.startree.aggregator.ValueAggregatorFactory;
import org.opensearch.index.codec.startree.builder.Dimension;
import org.opensearch.index.codec.startree.builder.SingleTreeBuilder;
import org.opensearch.index.codec.startree.builder.StarTreeBuilderUtils;
import org.opensearch.index.codec.startree.builder.StarTreeDocValuesIteratorFactory;
import org.opensearch.index.codec.startree.builder.StarTreeIndexConfig;
import org.opensearch.index.codec.startree.codec.StarTreeAggregatedValues;
import org.opensearch.index.codec.startree.node.StarTreeNode;
import org.opensearch.index.mapper.NumberFieldMapper;

import java.io.IOException;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Base class for star tree builder
 */
public abstract class BaseSingleTreeBuilder implements SingleTreeBuilder {
    private static final Logger logger = LogManager.getLogger(BaseSingleTreeBuilder.class);

    public static final int STAR_IN_DOC_VALUES_INDEX = 0;

    public final static long SECOND = 1000;
    public final static long MINUTE = 60 * SECOND;
    public final static long HOUR = 60 * 60 * SECOND;
    public final static long DAY = 24 * HOUR;
    public final static long MONTH = 30 * DAY;
    public final static long YEAR = 365 * DAY;

    public final String[] _dimensionsSplitOrder;
    public final Set<Integer> _skipStarNodeCreationForDimensions;
    // Name of the function-column pairs
    public final String[] _metrics;

    public final int _numMetrics;
    public final int _numDimensions;
    public int _numDocs;
    public int _totalDocs;
    public int _numNodes;
    public final int _maxLeafRecords;

    public final StarTreeBuilderUtils.TreeNode _rootNode = getNewNode();

    public IndexOutput indexOutput;
    public DocIdSetIterator[] _dimensionReaders;
    public DocIdSetIterator[] _metricReaders;

    public ValueAggregator[] _valueAggregators;
    public DocValuesConsumer _docValuesConsumer;
    public DocValuesProducer _docValuesProducer;

    private final StarTreeDocValuesIteratorFactory starTreeDocValuesIteratorFactory;
    private final StarTreeIndexConfig starTreeIndexConfig;
    private final SegmentWriteState segmentWriteState;

    protected BaseSingleTreeBuilder(
        StarTreeIndexConfig starTreeIndexConfig,
        DocValuesProducer docValuesProducer,
        DocValuesConsumer docValuesConsumer,
        SegmentWriteState state
    ) throws IOException {

        logger.info("Building in base star tree builder");

        String docFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, "stttree");
        logger.info("Star tree file name : {}", docFileName);

        indexOutput = state.directory.createOutput(docFileName, state.context);
        CodecUtil.writeIndexHeader(indexOutput, CodecService.STAR_TREE_CODEC, 0, state.segmentInfo.getId(), state.segmentSuffix);

        starTreeDocValuesIteratorFactory = new StarTreeDocValuesIteratorFactory();
        this.starTreeIndexConfig = starTreeIndexConfig;
        this.segmentWriteState = state;
        _docValuesConsumer = docValuesConsumer;
        _docValuesProducer = docValuesProducer;

        List<Dimension> dimensionsSplitOrder = starTreeIndexConfig.getDimensionsSplitOrder();
        _numDimensions = dimensionsSplitOrder.size();
        _dimensionsSplitOrder = new String[_numDimensions];
        // let's see how can populate this
        _skipStarNodeCreationForDimensions = new HashSet<>();
        _totalDocs = state.segmentInfo.maxDoc();
        _dimensionReaders = new DocIdSetIterator[_numDimensions];
        Set<Dimension> skipStarNodeCreationForDimensions = starTreeIndexConfig.getSkipStarNodeCreationForDimensions();

        for (int i = 0; i < _numDimensions; i++) {
            String dimension = dimensionsSplitOrder.get(i).getDimensionName();
            _dimensionsSplitOrder[i] = dimension;
            if (skipStarNodeCreationForDimensions.contains(dimensionsSplitOrder.get(i))) {
                _skipStarNodeCreationForDimensions.add(i);
            }
            FieldInfo dimensionFieldInfos = state.fieldInfos.fieldInfo(dimension);
            DocValuesType dimensionDocValuesType = state.fieldInfos.fieldInfo(dimension).getDocValuesType();
            _dimensionReaders[i] = starTreeDocValuesIteratorFactory.createIterator(
                dimensionDocValuesType,
                dimensionFieldInfos,
                docValuesProducer
            );
        }

        List<AggregationFunctionColumnPair> aggregationSpecs = new ArrayList<>(
            starTreeIndexConfig.getAggregationFunctionColumnPairs().values()
        );
        _numMetrics = aggregationSpecs.size();
        _metrics = new String[_numMetrics];
        _valueAggregators = new ValueAggregator[_numMetrics];
        _metricReaders = new DocIdSetIterator[_numMetrics];

        int index = 0;
        for (AggregationFunctionColumnPair aggregationFunctionColumnPair : aggregationSpecs) {
            _metrics[index] = aggregationFunctionColumnPair.toColumnName();
            _valueAggregators[index] = ValueAggregatorFactory.getValueAggregator(aggregationFunctionColumnPair.getFunctionType());
            // Ignore the column for COUNT aggregation function
            if (_valueAggregators[index].getAggregationType() != AggregationFunctionType.COUNT) {
                String metricName = aggregationFunctionColumnPair.getColumn();
                FieldInfo metricFieldInfos = state.fieldInfos.fieldInfo(metricName);
                DocValuesType metricDocValuesType = state.fieldInfos.fieldInfo(metricName).getDocValuesType();
                _metricReaders[index] = starTreeDocValuesIteratorFactory.createIterator(
                    metricDocValuesType,
                    metricFieldInfos,
                    docValuesProducer
                );
            }
            index++;
        }
        _maxLeafRecords = starTreeIndexConfig.getMaxLeafRecords();
    }

    /**
     * Appends a record to the star-tree.
     *
     * @param record Record to be appended
     */
    public abstract void appendRecord(Record record) throws IOException;

    /**
     * Returns the record of the given document Id in the star-tree.
     *
     * @param docId Document Id
     * @return Star-tree record
     */
    public abstract Record getStarTreeRecord(int docId) throws IOException;

    /**
     * Returns the dimension value of the given document and dimension Id in the star-tree.
     *
     * @param docId       Document Id
     * @param dimensionId Dimension Id
     * @return Dimension value
     */
    public abstract long getDimensionValue(int docId, int dimensionId) throws IOException;

    /**
     * Sorts and aggregates the records in the segment, and returns a record iterator for all the
     * aggregated records.
     *
     * <p>This method reads records from segment and generates the initial records for the star-tree.
     *
     * @param numDocs Number of documents in the segment
     * @return Iterator for the aggregated records
     */
    public abstract Iterator<Record> sortAndAggregateSegmentRecords(int numDocs) throws IOException;

    /**
     * Generates aggregated records for star-node.
     *
     * <p>This method will do the following steps:
     *
     * <ul>
     *   <li>Creates a temporary buffer for the given range of documents
     *   <li>Replaces the value for the given dimension Id to {@code STAR}
     *   <li>Sorts the records inside the temporary buffer
     *   <li>Aggregates the records with same dimensions
     *   <li>Returns an iterator for the aggregated records
     * </ul>
     *
     * @param startDocId  Start document Id in the star-tree
     * @param endDocId    End document Id (exclusive) in the star-tree
     * @param dimensionId Dimension Id of the star-node
     * @return Iterator for the aggregated records
     */
    public abstract Iterator<Record> generateRecordsForStarNode(int startDocId, int endDocId, int dimensionId) throws IOException;

    long[] getNextSegmentRecordDimensions() throws IOException {
        long[] dimensions = new long[_numDimensions];
        for (int i = 0; i < _numDimensions; i++) {
            try {
                _dimensionReaders[i].nextDoc();
            } catch (Exception e) {
                logger.info(e);
            }
            // TODO: Revisit this once
            dimensions[i] = getTimeStampVal(_dimensionsSplitOrder[i], starTreeDocValuesIteratorFactory.getNextValue(_dimensionReaders[i]));
        }
        return dimensions;
    }

    protected Record getNextSegmentRecord() throws IOException {
        long[] dimensions = getNextSegmentRecordDimensions();

        Object[] metrics = new Object[_numMetrics];
        for (int i = 0; i < _numMetrics; i++) {
            // Ignore the column for COUNT aggregation function
            if (_metricReaders[i] != null) {
                try {
                    _metricReaders[i].nextDoc();
                } catch (Exception e) {
                    // TODO : handle null values in columns
                    logger.info(e);
                }
                metrics[i] = starTreeDocValuesIteratorFactory.getNextValue(_metricReaders[i]);
            }
        }
        return new Record(dimensions, metrics);
    }

    /**
     * Merges a segment record (raw) into the aggregated record.
     *
     * <p>Will create a new aggregated record if the current one is {@code null}.
     *
     * @param aggregatedRecord Aggregated record
     * @param segmentRecord    Segment record
     * @return Merged record
     */
    protected Record mergeSegmentRecord(Record aggregatedRecord, Record segmentRecord) {
        // TODO: HANDLE KEYWORDS!
        if (aggregatedRecord == null) {
            long[] dimensions = Arrays.copyOf(segmentRecord._dimensions, _numDimensions);
            Object[] metrics = new Object[_numMetrics];
            for (int i = 0; i < _numMetrics; i++) {
                // TODO: fill this
                metrics[i] = _valueAggregators[i].getInitialAggregatedValue(segmentRecord._metrics[i]);
            }
            return new Record(dimensions, metrics);
        } else {
            for (int i = 0; i < _numMetrics; i++) {
                aggregatedRecord._metrics[i] = _valueAggregators[i].applyRawValue(aggregatedRecord._metrics[i], segmentRecord._metrics[i]);
            }
            return aggregatedRecord;
        }
    }

    /**
     * Merges a star-tree record (aggregated) into the aggregated record.
     *
     * <p>Will create a new aggregated record if the current one is {@code null}.
     *
     * @param aggregatedRecord Aggregated record
     * @param starTreeRecord   Star-tree record
     * @return Merged record
     */
    protected Record mergeStarTreeRecord(Record aggregatedRecord, Record starTreeRecord) {
        if (aggregatedRecord == null) {
            long[] dimensions = Arrays.copyOf(starTreeRecord._dimensions, _numDimensions);
            Object[] metrics = new Object[_numMetrics];
            for (int i = 0; i < _numMetrics; i++) {
                metrics[i] = _valueAggregators[i].cloneAggregatedValue((Long) starTreeRecord._metrics[i]);
            }
            return new Record(dimensions, metrics);
        } else {
            for (int i = 0; i < _numMetrics; i++) {
                aggregatedRecord._metrics[i] = _valueAggregators[i].applyAggregatedValue(
                    (Long) starTreeRecord._metrics[i],
                    (Long) aggregatedRecord._metrics[i]
                );
            }
            return aggregatedRecord;
        }
    }

    public abstract void build(List<StarTreeAggregatedValues> aggrList) throws IOException;

    public void build() throws IOException {
        long startTime = System.currentTimeMillis();
        logger.info("Tree of Aggregations build is a go with config {}", starTreeIndexConfig);

        Iterator<Record> recordIterator = sortAndAggregateSegmentRecords(_totalDocs);
        logger.info("Sorting and aggregating star-tree in ms : {}", (System.currentTimeMillis() - startTime));
        build(recordIterator);
        logger.info("Finished Building TOA in ms : {}", (System.currentTimeMillis() - startTime));
    }

    public void build(Iterator<Record> recordIterator) throws IOException {
        int numSegmentRecords = _totalDocs;

        while (recordIterator.hasNext()) {
            appendToStarTree(recordIterator.next());
        }
        int numStarTreeRecords = _numDocs;
        logger.info("Generated star tree docs : [{}] from segment docs : [{}]", numStarTreeRecords, numSegmentRecords);

        if (_numDocs == 0) {
            StarTreeBuilderUtils.serializeTree(indexOutput, _rootNode, _dimensionsSplitOrder, _numNodes);
            return;
        }

        constructStarTree(_rootNode, 0, _numDocs);
        int numRecordsUnderStarNode = _numDocs - numStarTreeRecords;
        logger.info(
            "Finished constructing star-tree, got [ {} ] tree nodes and [ {} ] records under star-node",
            _numNodes,
            numRecordsUnderStarNode
        );

        createAggregatedDocs(_rootNode);
        int numAggregatedRecords = _numDocs - numStarTreeRecords - numRecordsUnderStarNode;
        logger.info("Finished creating aggregated documents : {}", numAggregatedRecords);

        // Create doc values indices in disk
        createSortedDocValuesIndices(_docValuesConsumer);

        // Serialize and save in disk
        StarTreeBuilderUtils.serializeTree(indexOutput, _rootNode, _dimensionsSplitOrder, _numNodes);

        // TODO: Write star tree metadata aka config?

    }

    private void appendToStarTree(Record record) throws IOException {
        // TODO : uncomment this for sanity
        // boolean star = true;
        // for(long dim : record._dimensions) {
        // if(dim != StarTreeNode.ALL) {
        // star = false;
        // break;
        // }
        // }
        // if(star) {
        // System.out.println("======Overall sum =====" + (long) record._metrics[0]);
        // }
        // logger.info("Record : {}", record.toString());
        appendRecord(record);
        _numDocs++;
    }

    private StarTreeBuilderUtils.TreeNode getNewNode() {
        _numNodes++;
        return new StarTreeBuilderUtils.TreeNode();
    }

    private void constructStarTree(StarTreeBuilderUtils.TreeNode node, int startDocId, int endDocId) throws IOException {

        int childDimensionId = node._dimensionId + 1;
        if (childDimensionId == _numDimensions) {
            return;
        }

        // Construct all non-star children nodes
        node._childDimensionId = childDimensionId;
        Map<Long, StarTreeBuilderUtils.TreeNode> children = constructNonStarNodes(startDocId, endDocId, childDimensionId);
        node._children = children;

        // Construct star-node if required
        if (!_skipStarNodeCreationForDimensions.contains(childDimensionId) && children.size() > 1) {
            children.put(StarTreeNode.ALL, constructStarNode(startDocId, endDocId, childDimensionId));
        }

        // Further split on child nodes if required
        for (StarTreeBuilderUtils.TreeNode child : children.values()) {
            if (child._endDocId - child._startDocId > _maxLeafRecords) {
                constructStarTree(child, child._startDocId, child._endDocId);
            }
        }
    }

    private Map<Long, StarTreeBuilderUtils.TreeNode> constructNonStarNodes(int startDocId, int endDocId, int dimensionId)
        throws IOException {
        Map<Long, StarTreeBuilderUtils.TreeNode> nodes = new HashMap<>();
        int nodeStartDocId = startDocId;
        long nodeDimensionValue = getDimensionValue(startDocId, dimensionId);
        for (int i = startDocId + 1; i < endDocId; i++) {
            long dimensionValue = getDimensionValue(i, dimensionId);
            if (dimensionValue != nodeDimensionValue) {
                StarTreeBuilderUtils.TreeNode child = getNewNode();
                child._dimensionId = dimensionId;
                child._dimensionValue = nodeDimensionValue;
                child._startDocId = nodeStartDocId;
                child._endDocId = i;
                nodes.put(nodeDimensionValue, child);

                nodeStartDocId = i;
                nodeDimensionValue = dimensionValue;
            }
        }
        StarTreeBuilderUtils.TreeNode lastNode = getNewNode();
        lastNode._dimensionId = dimensionId;
        lastNode._dimensionValue = nodeDimensionValue;
        lastNode._startDocId = nodeStartDocId;
        lastNode._endDocId = endDocId;
        nodes.put(nodeDimensionValue, lastNode);
        return nodes;
    }

    private StarTreeBuilderUtils.TreeNode constructStarNode(int startDocId, int endDocId, int dimensionId) throws IOException {
        StarTreeBuilderUtils.TreeNode starNode = getNewNode();
        starNode._dimensionId = dimensionId;
        starNode._dimensionValue = StarTreeNode.ALL;
        starNode._startDocId = _numDocs;
        Iterator<Record> recordIterator = generateRecordsForStarNode(startDocId, endDocId, dimensionId);
        while (recordIterator.hasNext()) {
            appendToStarTree(recordIterator.next());
        }
        starNode._endDocId = _numDocs;
        return starNode;
    }

    private Record createAggregatedDocs(StarTreeBuilderUtils.TreeNode node) throws IOException {
        Record aggregatedRecord = null;
        if (node._children == null) {
            // For leaf node

            if (node._startDocId == node._endDocId - 1) {
                // If it has only one document, use it as the aggregated document
                aggregatedRecord = getStarTreeRecord(node._startDocId);
                node._aggregatedDocId = node._startDocId;
            } else {
                // If it has multiple documents, aggregate all of them
                for (int i = node._startDocId; i < node._endDocId; i++) {
                    aggregatedRecord = mergeStarTreeRecord(aggregatedRecord, getStarTreeRecord(i));
                }
                assert aggregatedRecord != null;
                for (int i = node._dimensionId + 1; i < _numDimensions; i++) {
                    aggregatedRecord._dimensions[i] = STAR_IN_DOC_VALUES_INDEX;
                }
                node._aggregatedDocId = _numDocs;
                appendToStarTree(aggregatedRecord);
            }
        } else {
            // For non-leaf node

            if (node._children.containsKey(StarTreeNode.ALL)) {
                // If it has star child, use the star child aggregated document directly
                for (StarTreeBuilderUtils.TreeNode child : node._children.values()) {
                    if (child._dimensionValue == StarTreeNode.ALL) {
                        aggregatedRecord = createAggregatedDocs(child);
                        node._aggregatedDocId = child._aggregatedDocId;
                    } else {
                        createAggregatedDocs(child);
                    }
                }
            } else {
                // If no star child exists, aggregate all aggregated documents from non-star children
                for (StarTreeBuilderUtils.TreeNode child : node._children.values()) {
                    aggregatedRecord = mergeStarTreeRecord(aggregatedRecord, createAggregatedDocs(child));
                }
                assert aggregatedRecord != null;
                for (int i = node._dimensionId + 1; i < _numDimensions; i++) {
                    aggregatedRecord._dimensions[i] = STAR_IN_DOC_VALUES_INDEX;
                }
                node._aggregatedDocId = _numDocs;
                appendToStarTree(aggregatedRecord);
            }
        }
        return aggregatedRecord;
    }

    private void createSortedDocValuesIndices(DocValuesConsumer docValuesConsumer) throws IOException {
        List<StarTreeDocValuesWriter> dimensionWriters = new ArrayList<>();
        List<StarTreeDocValuesWriter> metricWriters = new ArrayList<>();
        FieldInfo[] dimensionFieldInfoList = new FieldInfo[_dimensionReaders.length];
        FieldInfo[] metricFieldInfoList = new FieldInfo[_metricReaders.length];

        // star tree index field number
        int fieldNum = 0;
        for (int i = 0; i < _dimensionReaders.length; i++) {
            FieldInfo originalDimensionFieldInfo = segmentWriteState.fieldInfos.fieldInfo(_dimensionsSplitOrder[i]);
            final FieldInfo fi = new FieldInfo(
                _dimensionsSplitOrder[i] + "_dim",
                fieldNum,
                false,
                originalDimensionFieldInfo.omitsNorms(),
                originalDimensionFieldInfo.hasPayloads(),
                originalDimensionFieldInfo.getIndexOptions(),
                originalDimensionFieldInfo.getDocValuesType(),
                -1,
                originalDimensionFieldInfo.attributes(),
                originalDimensionFieldInfo.getPointDimensionCount(),
                originalDimensionFieldInfo.getPointIndexDimensionCount(),
                originalDimensionFieldInfo.getPointNumBytes(),
                originalDimensionFieldInfo.getVectorDimension(),
                originalDimensionFieldInfo.getVectorEncoding(),
                originalDimensionFieldInfo.getVectorSimilarityFunction(),
                false,
                originalDimensionFieldInfo.isParentField()
            );
            dimensionFieldInfoList[i] = fi;
            StarTreeDocValuesWriter starTreeDimensionDocValuesWriter = new StarTreeDocValuesWriter(
                originalDimensionFieldInfo.getDocValuesType(),
                getDocValuesWriter(originalDimensionFieldInfo.getDocValuesType(), fi, Counter.newCounter())
            );
            dimensionWriters.add(starTreeDimensionDocValuesWriter);
            fieldNum++;
        }
        for (int i = 0; i < _metricReaders.length; i++) {
            FieldInfo originalMetricFieldInfo = segmentWriteState.fieldInfos.fieldInfo(_metrics[i]);
            FieldInfo fi = new FieldInfo(
                _metrics[i] + "_metric",
                fieldNum,
                false,
                originalMetricFieldInfo.omitsNorms(),
                originalMetricFieldInfo.hasPayloads(),
                originalMetricFieldInfo.getIndexOptions(),
                originalMetricFieldInfo.getDocValuesType(),
                -1,
                originalMetricFieldInfo.attributes(),
                originalMetricFieldInfo.getPointDimensionCount(),
                originalMetricFieldInfo.getPointIndexDimensionCount(),
                originalMetricFieldInfo.getPointNumBytes(),
                originalMetricFieldInfo.getVectorDimension(),
                originalMetricFieldInfo.getVectorEncoding(),
                originalMetricFieldInfo.getVectorSimilarityFunction(),
                false,
                originalMetricFieldInfo.isParentField()
            );
            metricFieldInfoList[i] = fi;
            StarTreeDocValuesWriter starTreeMetricDocValuesWriter = new StarTreeDocValuesWriter(
                originalMetricFieldInfo.getDocValuesType(),
                getDocValuesWriter(originalMetricFieldInfo.getDocValuesType(), fi, Counter.newCounter())
            );
            metricWriters.add(starTreeMetricDocValuesWriter);
            fieldNum++;
        }

        // TODO: this needs to be extended for more than one keyword
        // Could be very heavy on RAM ?
        Map<String, Map<Long, BytesRef>> ordinalToSortedSetDocValueMap = new HashMap<>();
        for (int docId = 0; docId < _numDocs; docId++) {
            Record record = getStarTreeRecord(docId);
            for (int i = 0; i < record._dimensions.length; i++) {
                long val = record._dimensions[i];
                StarTreeDocValuesWriter starTreeDocValuesWriter = dimensionWriters.get(i);
                switch (starTreeDocValuesWriter.getDocValuesType()) {
                    case SORTED_SET:
                        if (val == -1) continue;
                        ((SortedSetDocValuesWriter) starTreeDocValuesWriter.getDocValuesWriter()).addValue(
                            docId,
                            getSortedSetDocValueBytes(val, i, ordinalToSortedSetDocValueMap)
                        );
                        break;
                    case SORTED_NUMERIC:
                        ((SortedNumericDocValuesWriter) starTreeDocValuesWriter.getDocValuesWriter()).addValue(docId, val);
                        break;
                    default:
                        throw new IllegalStateException("Unsupported doc values type");
                }
            }
            for (int i = 0; i < record._metrics.length; i++) {
                // TODO: Do we need to separate for different types? Maybe if we just have long or int, the parsing is much faster by type
                // cast instead of number fields mapper?
                try {
                    Number parse = NumberFieldMapper.NumberType.LONG.parse(record._metrics[i], true);
                    StarTreeDocValuesWriter starTreeDocValuesWriter = metricWriters.get(i);
                    ((SortedNumericDocValuesWriter) starTreeDocValuesWriter.getDocValuesWriter()).addValue(docId, parse.longValue());
                } catch (IllegalArgumentException e) {
                    logger.info("could not parse the value, exiting creation of star tree");
                }
            }
        }

        getStarTreeDocValueProducers(docValuesConsumer, dimensionWriters, dimensionFieldInfoList, _dimensionReaders);
        getStarTreeDocValueProducers(docValuesConsumer, metricWriters, metricFieldInfoList, _metricReaders);
    }

    private BytesRef getSortedSetDocValueBytes(long val, int i, Map<String, Map<Long, BytesRef>> ordinalToSortedSetDocValueMap)
        throws IOException {
        String dimensionName = _dimensionsSplitOrder[i];
        if (!ordinalToSortedSetDocValueMap.containsKey(dimensionName)) {
            ordinalToSortedSetDocValueMap.put(dimensionName, new HashMap<>());
        }
        BytesRef bytes;
        if (ordinalToSortedSetDocValueMap.get(dimensionName).containsKey(val)) {
            bytes = ordinalToSortedSetDocValueMap.get(dimensionName).get(val);
        } else {
            bytes = ((SortedSetDocValues) _dimensionReaders[i]).lookupOrd(val);
            ordinalToSortedSetDocValueMap.get(dimensionName).put(val, BytesRef.deepCopyOf(bytes));
        }
        return bytes;
    }

    private void getStarTreeDocValueProducers(
        DocValuesConsumer docValuesConsumer,
        List<StarTreeDocValuesWriter> docValuesWriters,
        FieldInfo[] fieldInfoList,
        DocIdSetIterator[] readers
    ) throws IOException {
        for (int i = 0; i < readers.length; i++) {
            final int increment = i;
            DocValuesProducer docValuesProducer = new EmptyDocValuesProducer() {
                @Override
                public SortedNumericDocValues getSortedNumeric(FieldInfo field) {
                    return ((SortedNumericDocValuesWriter) docValuesWriters.get(increment).getDocValuesWriter()).getDocValues();
                }

                @Override
                public SortedSetDocValues getSortedSet(FieldInfo field) {
                    return ((SortedSetDocValuesWriter) docValuesWriters.get(increment).getDocValuesWriter()).getDocValues();
                }
            };
            docValuesConsumer.addSortedNumericField(fieldInfoList[i], docValuesProducer);
        }
    }

    private DocValuesWriter<?> getDocValuesWriter(DocValuesType docValuesType, FieldInfo fi, Counter counter) {
        final ByteBlockPool.Allocator byteBlockAllocator = new ByteBlockPool.DirectTrackingAllocator(counter);
        final ByteBlockPool docValuesBytePool = new ByteBlockPool(byteBlockAllocator);
        switch (docValuesType) {
            case SORTED_SET:
                return new SortedSetDocValuesWriter(fi, counter, docValuesBytePool);
            case SORTED_NUMERIC:
                return new SortedNumericDocValuesWriter(fi, counter);
            default:
                throw new IllegalArgumentException("Unsupported DocValuesType: " + docValuesType);
        }
    }

    private long getTimeStampVal(final String fieldName, final long val) {
        long roundedDate = 0;
        long ratio = 0;

        switch (fieldName) {

            case "minute":
                ratio = ChronoField.MINUTE_OF_HOUR.getBaseUnit().getDuration().toMillis();
                roundedDate = DateUtils.roundFloor(val, ratio);
                return roundedDate;
            case "hour":
                ratio = ChronoField.HOUR_OF_DAY.getBaseUnit().getDuration().toMillis();
                roundedDate = DateUtils.roundFloor(val, ratio);
                return roundedDate;
            case "day":
                ratio = ChronoField.DAY_OF_MONTH.getBaseUnit().getDuration().toMillis();
                roundedDate = DateUtils.roundFloor(val, ratio);
                return roundedDate;
            case "month":
                roundedDate = DateUtils.roundMonthOfYear(val);
                return roundedDate;
            case "year":
                roundedDate = DateUtils.roundYear(val);
                return roundedDate;
            default:
                return val;
        }
    }

    public void close() throws IOException {
        boolean success = false;
        try {
            if (indexOutput != null) {
                indexOutput.writeInt(-1);
                CodecUtil.writeFooter(indexOutput); // write checksum
            }
            success = true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (success) {
                IOUtils.close(indexOutput);
            } else {
                IOUtils.closeWhileHandlingException(indexOutput);
            }
            indexOutput = null;
        }
    }

    /**
     * Star tree record
     */
    public static class Record {
        public final long[] _dimensions;
        public final Object[] _metrics;

        public Record(long[] dimensions, Object[] metrics) {
            _dimensions = dimensions;
            _metrics = metrics;
        }

        @Override
        public String toString() {
            return Arrays.toString(_dimensions) + " | " + Arrays.toString(_metrics);
        }
    }
}
