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
    
    int grpFieldByIndex;
    Type grpFieldType;
    int aggFieldByIndex;
    Op aggOp;
    Map<Field, Integer> aggMap;
    Map<Field, Integer> countMap;

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
        this.grpFieldByIndex = gbfield;
        this.grpFieldType = gbfieldtype;
        this.aggFieldByIndex = afield;
        this.aggOp = what;
        this.aggMap = new HashMap<Field, Integer>();
        this.countMap = new HashMap<Field, Integer>();

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field grpField = grpFieldByIndex == Aggregator.NO_GROUPING ? null : tup.getField(grpFieldByIndex);
        int aggField = ((IntField) tup.getField(aggFieldByIndex)).getValue();
        int count = countMap.getOrDefault(grpField, 0);
        int agg = aggMap.getOrDefault(grpField, 0);
        switch (aggOp) {
            case MIN:
                agg = count == 0 ? aggField : Math.min(agg, aggField);
                break;
            case MAX:
                agg = count == 0 ? aggField : Math.max(agg, aggField);
                break;
            case SUM:
            case AVG:
                agg += aggField;
                break;
            case COUNT:
                agg = count + 1;
                break;
            case SC_AVG:
                agg += aggField;
                break;
        }
        aggMap.put(grpField, agg);
        countMap.put(grpField, count + 1);
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
            this.iter = agg.aggMap.entrySet().iterator();
        }

        public IntegerAggregator getAgg() {
            return agg;
        }


        @Override
        public void open() throws DbException, TransactionAbortedException {
            iter = agg.aggMap.entrySet().iterator();
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
            iter = agg.aggMap.entrySet().iterator();
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
