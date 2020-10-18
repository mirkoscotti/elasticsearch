package org.elasticsearch.search.aggregations.metrics.strictcardinality;

import java.io.IOException;

import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.ParsedAggregation;

public class ParsedStrictCardinality extends ParsedAggregation implements StrictCardinality
{

    private long cardinalityValue;

    @Override
    public String getValueAsString() {
        return Double.toString((double) cardinalityValue);
    }

    @Override
    public double value() {
        return getValue();
    }

    @Override
    public long getValue() {
        return cardinalityValue;
    }

    @Override
    public String getType() {
        return StrictCardinalityAggregationBuilder.NAME;
    }

    private static final ObjectParser<ParsedStrictCardinality, Void> PARSER = new ObjectParser<>(
            ParsedStrictCardinality.class.getSimpleName(), true, ParsedStrictCardinality::new);

    static {
        declareAggregationFields(PARSER);
        PARSER.declareLong((agg, value) -> agg.cardinalityValue = value, CommonFields.VALUE);
    }

    public static ParsedStrictCardinality fromXContent(XContentParser parser, final String name) {
        ParsedStrictCardinality cardinality = PARSER.apply(parser, null);
        cardinality.setName(name);
        return cardinality;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params)
            throws IOException {
        builder.field(CommonFields.VALUE.getPreferredName(), cardinalityValue);
        return builder;
    }
}
