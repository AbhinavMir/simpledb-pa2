package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    TransactionId transactionId;
    DbIterator iter;
    int tableId;
    boolean inserted;
    TupleDesc resultTupleDesc;
    
    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
    	transactionId = t;
    	iter = child;
    	tableId = tableid;
    	inserted = false;
    	
    	String[] names = new String[] {"Inserted"};
    	Type[] types = new Type[] {Type.INT_TYPE};
    	resultTupleDesc = new TupleDesc(types, names);

    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return resultTupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	super.open();
    	iter.open();
    	inserted = false;
    }

    public void close() {
        // some code goes here
    	super.close();
    	iter.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	iter.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. iter returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting iter.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if (inserted) return null;
    	int insertedCount = 0;
    	while (iter.hasNext())
    	{
    		Tuple tup = iter.next();
    		try 
    		{
        		Database.getBufferPool().insertTuple(transactionId, tableId, tup);    			
    		}
    		catch (IOException e)
    		{
    			throw new DbException("IO Exception on tuple insertion");
    		}
    		insertedCount++;
    	}
    	Tuple resultTuple = new Tuple(resultTupleDesc);
    	resultTuple.setField(0, new IntField(insertedCount));
    	inserted = true;
    	return resultTuple;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] {iter};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        iter = children[0];
    }
}