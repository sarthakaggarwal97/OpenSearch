/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.startree.builder;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;

public interface DocValuesIteratorFactory {
    DocIdSetIterator createIterator(DocValuesType type, FieldInfo field, DocValuesProducer producer) throws IOException;

    long getNextValue(DocIdSetIterator iterator) throws IOException;

    int nextDoc(DocIdSetIterator iterator) throws IOException;
}
