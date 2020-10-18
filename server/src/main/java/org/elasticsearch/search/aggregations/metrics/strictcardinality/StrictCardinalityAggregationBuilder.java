package org.elasticsearch.search.aggregations.metrics.strictcardinality;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

public final class StrictCardinalityAggregationBuilder
	extends ValuesSourceAggregationBuilder.LeafOnly<ValuesSource, StrictCardinalityAggregationBuilder> {
	
	public static final String NAME = "strict_cardinality";
	
	public static final ObjectParser<StrictCardinalityAggregationBuilder, String> PARSER =
	        ObjectParser.fromBuilder(NAME, StrictCardinalityAggregationBuilder::new);
	static {
	    ValuesSourceAggregationBuilder.declareFields(PARSER, true, false, false);
	}
	
	public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
	    StrictCardinalityAggregatorFactory.registerAggregators(builder);
	}
	
	public StrictCardinalityAggregationBuilder(String name) {
	    super(name);
	}
	
	public StrictCardinalityAggregationBuilder(StrictCardinalityAggregationBuilder clone,
	                                     AggregatorFactories.Builder factoriesBuilder,
	                                     Map<String, Object> metadata) {
	    super(clone, factoriesBuilder, metadata);
	}
	
	@Override
	protected ValuesSourceType defaultValueSourceType() {
	    return CoreValuesSourceType.BYTES;
	}
	
	/**
	 * Read from a stream.
	 */
	public StrictCardinalityAggregationBuilder(StreamInput in) throws IOException {
	    super(in);
	}
	
	@Override
	protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
	    return new StrictCardinalityAggregationBuilder(this, factoriesBuilder, metadata);
	}
	
	@Override
	protected void innerWriteTo(StreamOutput out) throws IOException {
	    //Nothing to do
	}
	
	@Override
	protected boolean serializeTargetValueType(Version version) {
	    return true;
	}
	
	@Override
	protected StrictCardinalityAggregatorFactory innerBuild(QueryShardContext queryShardContext, ValuesSourceConfig config,
	                                                  AggregatorFactory parent,
	                                                  AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
	    return new StrictCardinalityAggregatorFactory(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
	}
	
	@Override
	public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
	    return builder;
	}
	
	@Override
	public int hashCode() {
	    return Objects.hash(super.hashCode());
	}
	
	@Override
	public boolean equals(Object obj) {
	    if (this == obj) return true;
	    if (obj == null || getClass() != obj.getClass()) return false;
	    if (super.equals(obj) == false) return false;
	    return true;
	}
	
	@Override
	public String getType() {
	    return NAME;
	}
}
