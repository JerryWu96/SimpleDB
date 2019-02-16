	package simpledb;

import java.io.Serializable;
import java.util.ArrayList; 
import java.util.NoSuchElementException;
import java.util.Iterator;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

	// For each TupleDesc we need a container to store a list of TDItem.
	public ArrayList<TDItem> TDList;
	  
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    	
    public Iterator<TDItem> iterator() {
        // some code goes here

        return TDList.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
    	TDList = new ArrayList<TDItem>(typeAr.length);
    	for (int i = 0; i < typeAr.length; i++)
    	{
        	TDItem t = new TDItem (typeAr[i], fieldAr[i]);
        	TDList.add(t);
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
    	TDList = new ArrayList<TDItem>(typeAr.length);
    	for (int i = 0; i < typeAr.length; i++)
    	{
        	TDItem t = new TDItem (typeAr[i], null);
        	TDList.add(t);
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
    	return TDList.size();
        // return 0;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
    	if (i < 0 || i > numFields())
    			throw new NoSuchElementException("i is not valid!");
    	return TDList.get(i).fieldName;
    }
  
    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here

    	if (i < 0 || i > numFields())
			throw new NoSuchElementException("i is not valid!");
        return TDList.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
    	if (name == null)
    		throw new NoSuchElementException("Current field name is null!");
    	boolean allNameNull = true;

        for (int i = 0; i < TDList.size(); i++)
        {
        	String curName = TDList.get(i).fieldName;
        	if (curName == null) 
        		continue;
        	allNameNull = false; 
        	if (curName.equals(name))
        		return i;
        }
        
        // throw exception when all field names are null
        if (allNameNull)
        	throw new NoSuchElementException("All field names are null");
        throw new NoSuchElementException("Name of the field is not found!"); // if it's not found
      
        
    	// some code goes here
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	int size = 0;
    	for (TDItem TD : TDList)
    		size += TD.fieldType.getLen(); // only fieldType??
    	return size;
        // some code goes here
        //return 0;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
    	int mergedSize = td1.numFields() + td2.numFields();
    	Type[] mergedType = new Type[mergedSize];
    	String[] mergedName = new String[mergedSize];
    	for (int i = 0; i < td1.numFields(); i++)
    	{
    		mergedType[i] = td1.getFieldType(i);
    		mergedName[i] = td1.getFieldName(i);
    	}
    	for (int j = 0; j < td2.numFields(); j++)
    	{
    		mergedType[j + td1.numFields()] = td2.getFieldType(j);
    		mergedName[j + td1.numFields()] = td2.getFieldName(j);
    	}
    	return new TupleDesc(mergedType, mergedName);
    	
    	
        // some code goes here    	
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	if (o == null) // cannot use o.equals(null)
    		return false;
    	if (!(o instanceof TupleDesc))
    		return false;
    	TupleDesc td = (TupleDesc) o;
    	if (this.numFields() == td.numFields() && this.getSize() == td.getSize())
    	{
    		for (int i = 0; i < this.numFields(); i++)
    			if (!this.getFieldType(i).equals(td.getFieldType(i)))
    				return false;
    		return true;
    	}
    	return false;
        // some code goes here
    	
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
        //return this.toString().hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
    	StringBuffer res = new StringBuffer();
    	for (int i = 0; i < this.numFields(); i++)
    	{
    		res.append(this.getFieldType(i) + "[" + i + "]");
    		res.append("(" + this.getFieldName(i) + ")");
    		if (i != this.numFields() - 1)
    			res.append(", ");
    	}    	
        // some code goes here
        return res.toString();
    }
}
