package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    int gbfield;
    Type gbfieldtype;
    int afield;
    Op what;
    HashMap<Field, Integer> groupValCount;

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if(what != Op.COUNT)
            throw new IllegalArgumentException("Only COUNT is supported");
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        groupValCount = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupField = gbfield == NO_GROUPING ? null : tup.getField(gbfield);
        if(groupValCount.containsKey(groupField))
            groupValCount.put(groupField, groupValCount.get(groupField) + 1);
        else
            groupValCount.put(groupField, 1);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        return new StringAggIterator(this);
    }

    public class StringAggIterator implements DbIterator
    {
        StringAggregator stringAgg;
        TupleDesc td;
        Iterator<HashMap.Entry<Field, Integer>> it;

        public StringAggIterator(StringAggregator sAgg) {
            stringAgg = sAgg;
            td = stringAgg.gbfield == NO_GROUPING ? new TupleDesc(new Type[]{Type.INT_TYPE}) : new TupleDesc(new Type[]{stringAgg.gbfieldtype, Type.INT_TYPE});
            it = stringAgg.groupValCount.entrySet().iterator();
        }


        @Override
        public void open() throws DbException, TransactionAbortedException {
            it = stringAgg.groupValCount.entrySet().iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
           return it.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return null;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            it = stringAgg.groupValCount.entrySet().iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return td;
        }

        @Override
        public void close() {

        }
    }

}
