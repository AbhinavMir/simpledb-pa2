package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    JoinPredicate joinpred;
    DbIterator child1;
    DbIterator child2;
    Tuple tup;


    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        this.joinpred = p;
        this.child1 = child1;
        this.child2 = child2;
    }

    public JoinPredicate getJoinPredicate() {
        return joinpred;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        return child1.getTupleDesc().getFieldName(joinpred.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        return child2.getTupleDesc().getFieldName(joinpred.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        return TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        child1.open();
        child2.open();
    }

    public void close() {
        child1.close();
        child2.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child1.rewind();
        child2.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // add exceptions
        if (child1 == null || child2 == null) {
            throw new DbException("child1 or child2 is null");
        }
        if (joinpred == null) {
            throw new DbException("join predicate is null");
        }
        if (tup == null) {
            if (child1.hasNext()) {
                tup = child1.next();
            } else {
                return null;
            }
        }
        while (child2.hasNext()) {
            Tuple tup2 = child2.next();
            if (joinpred.filter(tup, tup2)) {
                TupleDesc td = getTupleDesc();
                Tuple newTup = new Tuple(td);
                int i = 0;
                for (int j = 0; j < tup.getTupleDesc().numFields(); j++) {
                    newTup.setField(i, tup.getField(j));
                    i++;
                }
                for (int j = 0; j < tup2.getTupleDesc().numFields(); j++) {
                    newTup.setField(i, tup2.getField(j));
                    i++;
                }
                return newTup;
            }
        }
        child2.rewind();
        if (child1.hasNext()) {
            tup = child1.next();
            return fetchNext();
        } else {
            return null;
        }
    }

    @Override
    public DbIterator[] getChildren() {
        DbIterator[] children = new DbIterator[2];
        children[0] = child1;
        children[1] = child2;
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        child1 = children[0];
        child2 = children[1];
    }

}
