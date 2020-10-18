package org.elasticsearch.search.aggregations.metrics.strictcardinality;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;

public final class InternalStrictCardinality extends InternalNumericMetricsAggregation.SingleValue implements StrictCardinality
{
    private final CountCollector counts;

    InternalStrictCardinality(String name, CountCollector counts,
                              Map<String, Object> metaData) {
        super(name, metaData);
        this.counts = counts;
    }

    /**
     * Read from a stream.
     */
    public InternalStrictCardinality(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        if (in.readBoolean()) {
            counts = CountCollector.readSingletonFrom(in);
        } else {
            counts = null;
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        if (counts != null) {
            out.writeBoolean(true);
            counts.writeSingletonTo(0, out);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public String getWriteableName() {
        return StrictCardinalityAggregationBuilder.NAME;
    }

    @Override
    public double value() {
        return getValue();
    }

    @Override
    public long getValue() {
        return counts == null ? 0 : counts.cardinality(0);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        InternalStrictCardinality reduced = null;
        for (final InternalAggregation aggregation : aggregations) {
            final InternalStrictCardinality cardinality = (InternalStrictCardinality) aggregation;
            if (cardinality.counts != null) {
                if (reduced == null) {
                    reduced = new InternalStrictCardinality(name, new CountCollector(BigArrays.NON_RECYCLING_INSTANCE), getMetadata());
                }
                reduced.merge(cardinality);
            }
        }

        if (reduced == null) { // all empty
            return aggregations.get(0);
        } else {
            return reduced;
        }
    }

    public void merge(InternalStrictCardinality other) {
        assert counts != null && other != null;
        counts.merge(0, other.counts, 0);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        final long cardinality = getValue();
        builder.field(CommonFields.VALUE.getPreferredName(), cardinality);
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), counts.hashCode(0));
    }

    @Override
    public boolean equals(Object obj) {
        InternalStrictCardinality other = (InternalStrictCardinality) obj;
        return counts.equals(0, other.counts);
    }
}
