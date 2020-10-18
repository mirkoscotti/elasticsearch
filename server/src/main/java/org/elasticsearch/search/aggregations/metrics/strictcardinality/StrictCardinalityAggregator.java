package org.elasticsearch.search.aggregations.metrics.strictcardinality;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

/**
 * An aggregator that computes exact counts of unique values.
 */
public class StrictCardinalityAggregator extends NumericMetricsAggregator.SingleValue {
	
	private final ValuesSource valuesSource;
	private final CountCollector counts;
	
	public StrictCardinalityAggregator(
            String name,
            ValuesSourceConfig valuesSourceConfig,
            SearchContext context,
            Aggregator parent,
            Map<String, Object> metadata) throws IOException {
        super(name, context, parent, metadata);
        // TODO: Stop using nulls here
        this.valuesSource = valuesSourceConfig.hasValues() ? valuesSourceConfig.getValuesSource() : null;
        this.counts = valuesSource == null ? null : new CountCollector(context.bigArrays());
    }

	@Override
	public double metric(long owningBucketOrd) {
		return counts.cardinality(owningBucketOrd);
	}

	@Override
	public InternalAggregation buildAggregation(long owningBucketOrd) throws IOException {
		if (counts == null || counts.isEmptyBucket(owningBucketOrd)) {
            return buildEmptyAggregation();
        }
        // We need to build a copy because the returned Aggregation needs remain usable after
        // this Aggregator is released.
        return new InternalStrictCardinality(name, counts.singleton(owningBucketOrd),  metadata());
	}

	@Override
	protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
		if (valuesSource == null)
            return LeafBucketCollector.NO_OP_COLLECTOR;
        else if (valuesSource instanceof ValuesSource.Numeric)
            return new IntValueCollector(((ValuesSource.Numeric) valuesSource).longValues(ctx), counts);
        else
            return new ValueCollector(valuesSource.bytesValues(ctx), counts);
	}

	@Override
	public InternalAggregation buildEmptyAggregation() {
		return new InternalStrictCardinality(name, null,  metadata());
	}

    @Override
    protected void doClose() {
        if (counts != null)
            counts.close();
    }
}
