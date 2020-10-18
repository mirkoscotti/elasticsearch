package org.elasticsearch.search.aggregations.metrics.strictcardinality;

import java.io.IOException;

import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.LeafBucketCollector;

public final class ValueCollector   extends LeafBucketCollector
{
    private final SortedBinaryDocValues values;

    private final CountCollector counts;

    ValueCollector(final SortedBinaryDocValues values, final CountCollector counts)
    {
        this.values = values;
        this.counts = counts;
    }

    @Override
    public void collect(final int doc, final long bucket) throws IOException
    {
        if (values.advanceExact(doc)) {
            final int valueCount = values.docValueCount();
            final CountCollector.BytesRefSet z = counts.getCreate(bucket);
            final CountCollector.BytesRefSet interner = counts.getInterner();
            for (int i = 0; i < valueCount; i++) {
                z.add(interner.addClone(values.nextValue()));
            }
        }
    }
}
