package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private PageId pId;
    private int tupleNo;	
    private int hashCode; // add member m_hashCode

    
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        // some code goes here
    	this.pId = pid;
    	this.tupleNo = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
        // some code goes here
    	return this.tupleNo;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
    	return pId;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) 
    {
        // some code goes here
    	if(!(o instanceof RecordId))
            return false;
    	else 
    	 {
             RecordId rId = (RecordId)o;
             if(rId.getPageId().equals(this.getPageId()) && rId.tupleno() == this.tupleNo)
                 return true;
             else
                 return false;
         } 
    	 
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // some code goes here
    	this.hashCode = Integer.valueOf(String.valueOf(this.getPageId().hashCode()) + String.valueOf(this.tupleNo));
    	return this.hashCode;
        //throw new UnsupportedOperationException("implement this");

    }

}
