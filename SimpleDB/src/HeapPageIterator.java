package simpledb;

import java.util.*;

public class HeapPageIterator implements Iterator<Tuple> {

    private HeapPage heapPage; 
    private int numTuples;
    private int curTuple;

    /**
     * Constructor of the iterator for HeapPage
     */
    public HeapPageIterator(HeapPage heapPage) {
        this.heapPage = heapPage;
        this.curTuple = 0;
        this.numTuples = heapPage.occupiedTuples();
    }

    /** @return if there are more tuples followed current tuple */

    public boolean hasNext() {
        if (this.curTuple < this.numTuples) {
            return true;
        } else {
            return false;
        }
    }

    /** @return the next tuple */
    public Tuple next() {
        return heapPage.tuples[this.curTuple++];
    }


}
