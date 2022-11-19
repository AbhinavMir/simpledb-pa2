package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    int gbfield;
    Type gbfieldtype;
    int afield;
    Op what;
    HashMap<Field, Integer> groupAgg;
    HashMap<Field, Integer> groupCount;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groupAgg = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field group = gbfield != NO_GROUPING ? tup.getField(gbfield) : new IntField(NO_GROUPING);
        int val = ((IntField) tup.getField(afield)).getValue();

        if (groupAgg.containsKey(group)) {
            int agg = groupAgg.get(group);
            int count = groupCount.get(group);
            switch (what) {
            case MIN:
                agg = Math.min(agg, val);
                break;
            case MAX:
                agg = Math.max(agg, val);
                break;
            case SUM:
                agg += val;
                break;
            case AVG:
                agg += val;
                count++;
                break;
            case COUNT:
                count++;
                break;
            }
            groupAgg.put(group, agg);
            groupCount.put(group, count);
        } else {
            groupAgg.put(group, val);
            groupCount.put(group, 1);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */

    public DbIterator iterator()
    {
        return new IntegerAggregatorIterator(this);
    }

    public class IntegerAggregatorIterator implements DbIterator {

        IntegerAggregator agg;
        TupleDesc td;
        Iterator<HashMap.Entry<Field, Integer>> iter;

        public IntegerAggregatorIterator(IntegerAggregator agg) {
            this.agg = agg;
            this.td = td;
            this.iter = agg.groupAgg.entrySet().iterator();
        }

        public IntegerAggregator getAgg() {
            return agg;
        }


        @Override
        public void open() throws DbException, TransactionAbortedException {
            iter = agg.groupAgg.entrySet().iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return iter.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return null;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            iter = agg.groupAgg.entrySet().iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return this.td;
        }

        @Override
        public void close() {
            agg = null;
            close();
        }
    }

}
