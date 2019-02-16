package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

	// newly added private member variables:
	private TupleDesc tpDesc;
	private Field[] tpField;
	private RecordId rId;
	
    private static final long serialVersionUID = 1L;
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
    	
    	//try + catch? assert?
    	assert td.numFields() > 0;
        	tpDesc = td;
        	tpField = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
    	return this.tpDesc;
        // some code goes here
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
    	return this.rId;
        // some code goes here
  
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
    	this.rId = rid;
        // some code goes here
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
    	// assert ?
    	assert (i >= 0 && i < tpField.length);
    	tpField[i] = f;
        // some code goes here
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
    	// assert ?
    	assert (i >= 0 && i < tpField.length);
		return tpField[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
    	StringBuffer res = new StringBuffer();
    	for (int i = 0; i < tpField.length; i++)
    		res.append (tpField[i] + "\t");
        // some code goes here
    	return res.toString();
    	
    	//implement this following:
        //throw new UnsupportedOperationException("Implement this");
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
    	return Arrays.asList(tpField).iterator();
    	
        // some code goes here
    }

    /**
     * reset the TupleDesc of this tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
    	this.tpDesc = td;
    	this.tpField = new Field[td.getSize()];
        // some code goes here
    }
}
