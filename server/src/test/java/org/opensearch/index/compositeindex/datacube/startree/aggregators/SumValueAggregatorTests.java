/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.apache.lucene.util.NumericUtils;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.test.OpenSearchTestCase;

public class SumValueAggregatorTests extends OpenSearchTestCase {

    private final SumValueAggregator aggregator = new SumValueAggregator();

    public void testGetAggregationType() {
        assertEquals(MetricStat.SUM.getTypeName(), aggregator.getAggregationType().getTypeName());
    }

    public void testGetAggregatedValueType() {
        assertEquals(SumValueAggregator.AGGREGATED_VALUE_TYPE, aggregator.getAggregatedValueType());
    }

    public void testGetInitialAggregatedValue() {
        assertEquals(1.0, aggregator.getInitialAggregatedValue(1L), 0.0);
        assertThrows(NullPointerException.class, () -> aggregator.getInitialAggregatedValue(null));
    }

    public void testApplySegmentRawValue() {
        assertEquals(5.0, aggregator.applySegmentRawValue(2.0, 3L), 0.0);
        assertThrows(NullPointerException.class, () -> aggregator.applySegmentRawValue(3.14, null));
    }

    public void testApplyAggregatedValue() {
        assertEquals(5.0, aggregator.applyAggregatedValue(2.0, 3.0), 0.0);
        assertEquals(7.28, aggregator.applyAggregatedValue(3.14, 4.14), 0.0000001);
    }

    public void testCloneAggregatedValue() {
        assertEquals(3.14, aggregator.cloneAggregatedValue(3.14), 0.0);
    }

    public void testGetMaxAggregatedValueByteSize() {
        assertEquals(Double.BYTES, aggregator.getMaxAggregatedValueByteSize());
    }

    public void testConvertAggregationTypeToSortableLongValue() {
        SumValueAggregator aggregator = new SumValueAggregator();
        assertEquals(NumericUtils.doubleToSortableLong(3.14), aggregator.convertAggregationTypeToSortableLongValue(3.14), 0.0);
    }

    public void testConvertSortableLongToAggregatedTypeValue() {
        SumValueAggregator aggregator = new SumValueAggregator();
        assertEquals(NumericUtils.sortableLongToDouble(3L), aggregator.convertSortableLongToAggregatedTypeValue(3L), 0.0);
    }
}
