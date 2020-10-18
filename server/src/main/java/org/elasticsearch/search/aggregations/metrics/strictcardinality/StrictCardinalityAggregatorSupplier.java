package org.elasticsearch.search.aggregations.metrics.strictcardinality;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.support.AggregatorSupplier;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

public interface StrictCardinalityAggregatorSupplier  extends AggregatorSupplier {
    Aggregator build(String name,
            ValuesSourceConfig valuesSourceConfig,
            SearchContext context,
            Aggregator parent,
            Map<String, Object> metadata) throws IOException;
}
