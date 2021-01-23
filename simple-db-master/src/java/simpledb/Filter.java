package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private Predicate p;
    private OpIterator child;
    private OpIterator[] children;
    
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
    	this.p = p;
    	this.child = child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return this.p;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	super.open();
    	child.open();
    }

    public void close() {
        // some code goes here
    	child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.close();
    	super.close();
    	super.open();
    	child.open();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
    	if (!child.hasNext()) {
    		return null;
    	} else {
    		Tuple t = child.next();
    		if (t == null) return null;
        	if (p.filter(t)) return t;
        	else {
        		return fetchNext();
        	}
    	}
    	
       
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
    	if (this.children != null) {
    	child = this.children[0];
    	}
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    	this.children = children;
    }

}
