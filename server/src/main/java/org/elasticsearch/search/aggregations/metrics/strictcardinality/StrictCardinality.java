package org.elasticsearch.search.aggregations.metrics.strictcardinality;

import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;

/**
 * An aggregation that computes exact numbers of unique terms.
 */
public interface StrictCardinality extends NumericMetricsAggregation.SingleValue {

    /**
     * The number of unique terms.
     */
    long getValue();

}
