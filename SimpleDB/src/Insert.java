package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    // Added updateInsertCount function to update the member variable insertCount
    // Added getCountTuple to return the TupleDesc needed. Called by fetchNext.

    // Added member variables
    private TransactionId t;
    private DbIterator child;
    private int tableId;
    private int insertCount;
    private TupleDesc td = new TupleDesc(new Type[] { Type.INT_TYPE });
    //private boolean isFetched = false;
    
    
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */

    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        // some code goes here
    	if (!(Database.getCatalog().getTupleDesc(tableId).equals(child.getTupleDesc()))) {
      	throw new DbException("Unmached TupleDesc bwtween table and the child!");
      }
      
      this.t = t;
      this.child = child;
      this.tableId = tableId;
    }

    // Added updateInsertCount function to update the member variable insertCount. called by open(), close() and rewind();
    private void updateInsertCount() throws DbException, TransactionAbortedException {
        int count = 0;
        while (this.child.hasNext()) {
        	try {
				Database.getBufferPool().insertTuple(this.t, this.tableId, child.next());
				count++;
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
        this.insertCount = count;
    }


    
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.open();
        updateInsertCount();
        super.open();

    }

    public void close() {
        // some code goes here
    	 super.close();
         this.child.close();
         this.insertCount = -1;
    }
    
    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	this.child.rewind();
    	updateInsertCount();
    	
    }
    
    // Added getCountTuple to return the TupleDesc needed. Called by fetchNext.
    private Tuple getCountTuple() {
        Type[] types = { Type.INT_TYPE };
        TupleDesc insertedCountTupleDesc = new TupleDesc(types);
        Tuple insertedCountTuple = new Tuple(insertedCountTupleDesc);
        insertedCountTuple.setField(0, new IntField(this.insertCount));
        return insertedCountTuple;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if (this.insertCount != -1) {
    		Tuple countTupleResult = getCountTuple();
    		this.insertCount = -1;
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
