package org.elasticsearch.search.aggregations.metrics.strictcardinality;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregatorSupplier;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.internal.SearchContext;

class StrictCardinalityAggregatorFactory extends ValuesSourceAggregatorFactory {

    StrictCardinalityAggregatorFactory(String name, ValuesSourceConfig config,
                                    QueryShardContext queryShardContext,
                                    AggregatorFactory parent,
                                    AggregatorFactories.Builder subFactoriesBuilder,
                                    Map<String, Object> metadata) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(StrictCardinalityAggregationBuilder.NAME, CoreValuesSourceType.ALL_CORE,
            (StrictCardinalityAggregatorSupplier) StrictCardinalityAggregator::new);
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext,
                                            Aggregator parent,
                                            Map<String, Object> metadata) throws IOException {
        return new StrictCardinalityAggregator(name, config, searchContext, parent, metadata);
    }

    @Override
    protected Aggregator doCreateInternal(SearchContext searchContext,
                                          Aggregator parent,
                                          CardinalityUpperBound cardinality,
                                          Map<String, Object> metadata) throws IOException {
        AggregatorSupplier aggregatorSupplier = queryShardContext.getValuesSourceRegistry().getAggregator(config,
            StrictCardinalityAggregationBuilder.NAME);
        if (aggregatorSupplier instanceof StrictCardinalityAggregatorSupplier == false) {
            throw new AggregationExecutionException("Registry miss-match - expected StrictCardinalityAggregatorSupplier, found [" +
                aggregatorSupplier.getClass().toString() + "]");
        }
        StrictCardinalityAggregatorSupplier strictCardinalityAggregatorSupplier = (StrictCardinalityAggregatorSupplier) aggregatorSupplier;
        return strictCardinalityAggregatorSupplier.build(name, config, searchContext, parent, metadata);
    }
}
