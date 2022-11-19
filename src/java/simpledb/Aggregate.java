package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    private Aggregator aggregator;
    private DbIterator aggIterator;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */

    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	    this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;

        Type gbFieldType = gfield != Aggregator.NO_GROUPING ? child.getTupleDesc().getFieldType(gfield) : null;

        switch(child.getTupleDesc().getFieldType(afield)) {
            case INT_TYPE:
                aggregator = new IntegerAggregator(gfield, gbFieldType, afield, this.aop);
                break;
            case STRING_TYPE:
                aggregator = new StringAggregator(gfield, gbFieldType, afield, this.aop);
                break;
            default:
                System.err.println("Unsupported Aggregator Type");
                System.exit(1);
        }
        aggIterator = aggregator.iterator();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	    return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	    if(this.gfield != Aggregator.NO_GROUPING)
            return this.child.getTupleDesc().getFieldName(this.gfield);
	    return null;
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	    return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	    return this.child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	    return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	    return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {

	    child.open();

        while(child.hasNext())
            this.aggregator.mergeTupleIntoGroup(child.next());

        child.close();
        aggIterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {

        try{
            boolean hasNext = aggIterator.hasNext();
            if(hasNext)
                return aggIterator.next();
        }
        catch(NoSuchElementException e) {return null;}
    }

    public void rewind() throws DbException, TransactionAbortedException {
	    aggIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc childDesc = child.getTupleDesc();
        String aggCol = aop.toString() + "(" + childDesc.getFieldName(afield) + ")";
        String groupByCol = gfield != Aggregator.NO_GROUPING ? childDesc.getFieldName(gfield) : null;

        String[] fieldNames;
        Type[] fieldTypes;
        if (gfield == Aggregator.NO_GROUPING){
            fieldTypes = new Type[] {Type.INT_TYPE};
            fieldNames = new String[] {aggCol};
        } else {
            fieldTypes = new Type[] {child.getTupleDesc().getFieldType(gfield), Type.INT_TYPE};
            fieldNames = new String[] {groupByCol, aggCol};
        }

        return new TupleDesc(fieldTypes, fieldNames);
    }

    public void close() {
	    child.close();
        aggIterator.close();
        super.close();
    }

    @Override
    public DbIterator[] getChildren() {
	return new DbIterator[] {child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if(children.length == 0)
            throw new IllegalArgumentException("Not enough Elements");
        child = children[0];
    }
}
