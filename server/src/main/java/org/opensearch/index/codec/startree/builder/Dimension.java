/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.startree.builder;

import java.util.HashMap;
import java.util.Map;

public class Dimension {

    private String dimensionName;
    private Map<String, String> dimensionAttributes;

    public Dimension(String dimensionName, Map<String, String> dimensionAttributes) {
        this.dimensionName = dimensionName;
        this.dimensionAttributes = dimensionAttributes;
    }

    public Dimension(String dimensionName) {
        this.dimensionName = dimensionName;
        this.dimensionAttributes = new HashMap<>();
    }

    public String getDimensionName() {
        return dimensionName;
    }

    public void setDimensionName(String dimensionName) {
        this.dimensionName = dimensionName;
    }

    public Map<String, String> getDimensionAttributes() {
        return dimensionAttributes;
    }

    public void setDimensionAttributes(Map<String, String> dimensionAttributes) {
        this.dimensionAttributes = dimensionAttributes;
    }
}
