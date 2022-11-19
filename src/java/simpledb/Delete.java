package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    static final long serialVersionUID = 1L;
    TransactionId transactionId;
    DbIterator it;
    TupleDesc resultTupleDesc;
    boolean deleted;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.transactionId = t;
        this.it = child;
        this.resultTupleDesc = new TupleDesc(new Type[] { Type.INT_TYPE });
        this.deleted = false;

        String[] deletedNames = new String[] { "Deleted" };
        Type[] deletedTypes = new Type[] { Type.INT_TYPE };
        this.resultTupleDesc = new TupleDesc(deletedTypes, deletedNames);
    }

    public TupleDesc getTupleDesc() {
        return this.resultTupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        this.it.open();
        deleted = false;
    }

    public void close() {
        super.close();
        this.it.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.it.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    
        if (deleted) return null;
        int deletedCount = 0;
        while (it.hasNext()) {
            Tuple tup = it.next();
            try {
                Database.getBufferPool().deleteTuple(transactionId, tup);
            } catch (IOException e) {
                throw new DbException("IO tup del exp");
            }
            deletedCount++;
        }

        Tuple resultTuple = new Tuple(resultTupleDesc);
        resultTuple.setField(0, new IntField(deletedCount));
        deleted = true;
        return resultTuple;

    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] { this.it };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes her
        it = children[0];
    }

}
