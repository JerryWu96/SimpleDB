package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {
	
	private static final long serialVersionUID = 1L;
    
	private HeapFile file;
    private TransactionId tid;
	private Iterator<Tuple> i;
    private boolean open;
    private int currentPage = 0;
    
    public HeapFileIterator(HeapFile file, TransactionId tid) {
    	this.file = file;
    	this.tid = tid;
    }
    
	@Override
	public void open() throws DbException, TransactionAbortedException {
		this.open = true;
		this.currentPage = 0;
		this.i = ((HeapPage) (Database
								  .getBufferPool()
								  .getPage(
									  this.tid,
									  new HeapPageId(
										  this.file.getId(),
										  this.currentPage),
										  Permissions.READ_ONLY)))
								  .iterator();
	}

	@Override
	public boolean hasNext() throws DbException, TransactionAbortedException {
		if (!this.open) {
			return false;
		}
		
		while (!(this.i.hasNext())) {
			if (this.currentPage == (this.file.numPages() - 1)) {
				return false;
			}
			
			this.currentPage++;
			this.i = ((HeapPage) (Database
									  .getBufferPool()
									  .getPage(
										  this.tid,
										  new HeapPageId(
											  this.file.getId(),
											  this.currentPage),
											  Permissions.READ_ONLY)))
									  .iterator();
		}
		
		return true;
	}

	@Override
	public Tuple next() throws DbException, TransactionAbortedException,
			NoSuchElementException {
		if (!this.open) {
			throw new NoSuchElementException("Not opened yet.");
		}
		
		if (this.i.hasNext()) {
			return this.i.next();
		}
		
		return null;
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		if (!this.open) {
			throw new DbException("Not opened yet.");
		}
		
		this.open();
	}

	@Override
	public void close() {
		this.open = false;
	}

}
