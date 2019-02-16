package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

	private File file;
	private TupleDesc td;
	
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	HeapPage res = null;
    	byte[] buffer = new byte[BufferPool.getPageSize()];
    	long offset = pid.pageNumber() * BufferPool.getPageSize();

    	try {
			RandomAccessFile f = new RandomAccessFile(this.file, "r");
			f.seek(offset);
			f.read(buffer, 0, BufferPool.getPageSize());
			f.close();
			
			res = new HeapPage(
						 new HeapPageId(
								 pid.getTableId(),
								 pid.pageNumber()),
						 buffer);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

    	return res;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
    	try {
			RandomAccessFile f = new RandomAccessFile(this.file, "rw");
			f.seek(
				 (long) page.getId().pageNumber() * BufferPool.getPageSize());
			f.write(page.getPageData(), 0, BufferPool.getPageSize());
			f.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (Math.ceil((1.0 * this.file.length()) /
        				 BufferPool.getPageSize()));
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	HeapPage page = null;
    	int pageNo = 0;
    	while (pageNo < this.numPages()) {
    		page =
    			(HeapPage) Database
    						   .getBufferPool()
    						   .getPage(
    							   tid,
    							   new HeapPageId(this.getId(), pageNo),
    							   Permissions.READ_WRITE);

			if (page.getNumEmptySlots() > 0) {
				break;
			}

			// Release the lock on the page early. This violates 2PL, but works
			// because the scan did not modify any data.
			Database.getBufferPool().releasePage(tid, page.getId());

			pageNo++;
    	}

    	if (pageNo == this.numPages()) {
    		// Create a new HeapPage.
    		page = new HeapPage(
    				   new HeapPageId(
    					   this.getId(),
    					   pageNo),
    				   HeapPage.createEmptyPageData());

    		// Write it to disk.
    		this.writePage(page);

    		// Now reread the page from the buffer pool.
    		page =
    			(HeapPage) Database
    						   .getBufferPool()
    						   .getPage(
    							   tid,
    							   new HeapPageId(this.getId(), pageNo),
    							   Permissions.READ_WRITE);
    	}

    	page.insertTuple(t);

    	ArrayList<Page> res = new ArrayList<Page>();
    	res.add(page);
    	return res;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
    	HeapPage page =
    		(HeapPage) Database
    					   .getBufferPool()
    					   .getPage(
    						   tid,
    						   t.getRecordId().getPageId(),
    						   Permissions.READ_WRITE);
    	page.deleteTuple(t);

    	// No need to write here because the tuple is already invalidated in
    	// memory, so it will be overwritten (and persisted to disk) later when
    	// a new tuple is inserted in its place.

    	ArrayList<Page> res = new ArrayList<Page>();
    	res.add(page);
    	return res;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
		return new HeapFileIterator(this, tid);
    }

}
