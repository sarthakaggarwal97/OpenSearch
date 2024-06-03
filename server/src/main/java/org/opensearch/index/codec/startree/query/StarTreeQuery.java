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
package org.opensearch.index.codec.startree.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Accountable;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.codec.StarTreeReader;
import org.opensearch.index.codec.startree.codec.StarTreeAggregatedValues;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Query class for querying star tree data structure
 */
public class StarTreeQuery extends Query implements Accountable {

    Map<String, List<Predicate<Long>>> compositePredicateMap;
    Set<String> groupByColumns;

    public StarTreeQuery(Map<String, List<Predicate<Long>>> compositePredicateMap, Set<String> groupByColumns) {
        this.compositePredicateMap = compositePredicateMap;
        this.groupByColumns = groupByColumns;
    }

    @Override
    public String toString(String field) {
        return null;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public boolean equals(Object obj) {
        return sameClassAs(obj);
    }

    @Override
    public int hashCode() {
        return classHash();
    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                // logger.info("Query ::: scorer ::: size: {}", compositePredicateMap.size());

                // Object obj = context.reader().getAggregatedDocValues();
                SegmentReader reader = Lucene.segmentReader(context.reader());
                if (!(reader.getDocValuesReader() instanceof StarTreeReader)) return null;
                StarTreeReader starTreeDocValuesReader = (StarTreeReader) reader.getDocValuesReader();
                StarTreeAggregatedValues obj = starTreeDocValuesReader.getStarTreeValues();

                // context.reader().getFieldInfos().fieldInfo("clientip");
                // SortedSetDocValues field = context.reader().getSortedSetDocValues("clientip");

                DocIdSetIterator result = null;
                if (obj != null) {
                    StarTreeAggregatedValues val = (StarTreeAggregatedValues) obj;
                    StarTreeFilter filter = new StarTreeFilter(
                        val,
                        compositePredicateMap != null ? compositePredicateMap : new HashMap<>(),
                        groupByColumns
                    );
                    result = filter.getStarTreeResult();
                }
                return new ConstantScoreScorer(this, score(), scoreMode, result);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }
        };
    }
}
