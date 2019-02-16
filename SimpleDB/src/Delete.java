package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    // Added getCountTuple to return the TupleDesc needed. Called by fetchNext.
    // Added updatedeleteCount function to update the member variable deleteCount. called by open(), close() and rewind();
    // Added member variables
    private TransactionId t;
    private DbIterator child;
    // similar to Insert, here using deleteCount to help decide the current status.
    private int deleteCount;
    private TupleDesc td = new TupleDesc(new Type[] { Type.INT_TYPE });

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
        // some code goes here
    	this.t = t;
        this.child = child;
        this.deleteCount = -1;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }
    
    // Added updatedeleteCount function to update the member variable deleteCount. called by open(), close() and rewind();
    private void updateDeleteCount() throws DbException, TransactionAbortedException {
        int count = 0;
        while (this.child.hasNext()) {
        	try {
				Database.getBufferPool().deleteTuple(this.t, child.next());
				count++;
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
        this.deleteCount = count;
    }


    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	  this.child.open();
          updateDeleteCount();
          super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        this.child.close();
        this.deleteCount = -1;

    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	this.child.rewind();
    	updateDeleteCount();

    }

    // Added getCountTuple to return the TupleDesc needed. Called by fetchNext.
    private Tuple getCountTuple() {
        Type[] types = { Type.INT_TYPE };
        TupleDesc deletedCountTupleDesc = new TupleDesc(types);
        Tuple deletedCountTuple = new Tuple(deletedCountTupleDesc);
        deletedCountTuple.setField(0, new IntField(this.deleteCount));
        return deletedCountTuple;
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
        // some code goes here
    	if (this.deleteCount != -1) {
    		Tuple countTupleResult = getCountTuple();
    		this.deleteCount = -1;
    		return countTupleResult;
    	} else {
    		return null;
    	}
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
    	return new DbIterator[] { this.child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	if (this.child != children[0]) {
    	    this.child = children[0];
    	}
    }
}
