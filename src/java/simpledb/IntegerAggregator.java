package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gfi;
    private Type gfiType;
    private int aggregateFieldIndex;
    private Op op;
    private HashMap<Field,Integer> aggregateData;
    private HashMap<Field,Integer> count;
    
    
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
    	gfi = gbfield;
    	gfiType = gbfieldtype;
    	aggregateFieldIndex = afield;
    	op = what;
    	aggregateData = new HashMap<Field, Integer>();
    	count = new HashMap<Field, Integer>();
    }

    private int initialData()
    {
    	switch(op)
    	{
	    	case MIN: return Integer.MAX_VALUE;
	    	case MAX: return Integer.MIN_VALUE;
	    	case SUM: case COUNT: case AVG: return 0;
	    	default: return 0; // shouldn't reach here
    	}
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
    	Field tupleGroupByField = (gfi == Aggregator.NO_GROUPING) ? null : tup.getField(gfi);
    	
    	if (!aggregateData.containsKey(tupleGroupByField))
    	{
    		aggregateData.put(tupleGroupByField, initialData());
    		count.put(tupleGroupByField, 0);
    	}
    	
    	int tupleValue = ((IntField) tup.getField(aggregateFieldIndex)).getValue();
    	int currentValue = aggregateData.get(tupleGroupByField);
    	int currentCount = count.get(tupleGroupByField);
    	int newValue = currentValue;
    	switch(op)
    	{
    		case MIN: 
    			newValue = (tupleValue > currentValue) ? currentValue : tupleValue;
    			break;
    		case MAX:
    			newValue = (tupleValue < currentValue) ? currentValue : tupleValue;
    			break;
    		case SUM: case AVG:
    			// can't calculate average until all the tuples are in
    			// In the mean time, keep track of sum and count and 
    			// calculate the averages in the iterator
    			count.put(tupleGroupByField, currentCount+1);
    			newValue = tupleValue + currentValue;
    			break;
    		case COUNT:
    			newValue = currentValue + 1;
    			break;
			default:
				break;
    	}
    	aggregateData.put(tupleGroupByField, newValue);
    }

    private TupleDesc createGroupByTupleDesc()
    {
    	String[] names;
    	Type[] types;
    	if (gfi == Aggregator.NO_GROUPING)
    	{
    		names = new String[] {"aggregateValue"};
    		types = new Type[] {Type.INT_TYPE};
    	}
    	else
    	{
    		names = new String[] {"groupValue", "aggregateValue"};
    		types = new Type[] {gfiType, Type.INT_TYPE};
    	}
    	return new TupleDesc(types, names);
    }
    
    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	ArrayList<Tuple> tuples = new ArrayList<Tuple>();
    	TupleDesc tupledesc = createGroupByTupleDesc();
    	Tuple addMe;
    	for (Field group : aggregateData.keySet())
    	{
    		int aggregateVal;
    		if (op == Op.AVG)
    		{
    			aggregateVal = aggregateData.get(group) / count.get(group);
    		}
    		else
    		{
    			aggregateVal = aggregateData.get(group);
    		}
    		addMe = new Tuple(tupledesc);
    		if (gfi == Aggregator.NO_GROUPING){
    			addMe.setField(0, new IntField(aggregateVal));
    		}
    		else {
        		addMe.setField(0, group);
        		addMe.setField(1, new IntField(aggregateVal));    			
    		}
    		tuples.add(addMe);
    	}
    	return new TupleIterator(tupledesc, tuples);
    }

}
