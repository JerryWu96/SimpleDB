package simpledb;

import java.io.*;
import java.util.concurrent.*;
import java.util.*;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;
    private int pagesLimit;
    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    // Hashmap with the mapping of pageid and page.
	private HashMap<PageId, Page> pool;
	
	
	//Hashtable to keep the recently used page
	//latest page added will have priority 0
	//last used page = max priority (evicted)
	
	private HashMap<PageId, Integer> recent;
    
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
    	//Constructor
    	this.pagesLimit = numPages;
    	pool = new HashMap<PageId, Page>();
    	recent = new HashMap<PageId, Integer>();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException 
    {
    	
    	//Look up page in the buffer pool
    	//If it contains the page then update it's priority and add it to LRU cache
    	if(pool.containsKey(pid)){
            updatePriority();
            recent.put(pid,0);
            return pool.get(pid);

        }
    	//Insufficient space in buffer pool
        else if(pool.size()>=pagesLimit){
            evictPage();
        }
    	
    	//If page does not exist in buffer pool 
    	//retrieve the page from database
    	//And then add it to buffer pool
    	Catalog c = Database.getCatalog();
    	DbFile file = c.getDatabaseFile(pid.getTableId());  
        Page p = file.readPage(pid); 
        pool.put(pid,p);   
        updatePriority();
        recent.put(pid, 0);
        return p;
        
        // throw new DbException("page requested not in bufferpool or disk");
        // some code goes here
    } 	
    
    //Update priority of all pages when a new page is added
    public void updatePriority()
    {
        if(recent.size() == 0)
          return;
        if(recent.size() > 0)
        {
            for (PageId iterator : recent.keySet()){
                int priority = recent.get(iterator);
                priority++;
                recent.put(iterator,priority);
            }
         }
    }
    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
    	ArrayList<Page> dirtiedPages = dbFile.insertTuple(tid, t);
    	for (Page currPage : dirtiedPages) {
    		currPage.markDirty(true, tid);
    		if (!this.pool.containsKey(currPage.getId())) {
        		while (this.pool.size() >= this.pagesLimit) {
        			evictPage();
        		}
    		}
    		this.pool.put(currPage.getId(), currPage);
 
    	
    	}
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	DbFile databaseFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
    	ArrayList<Page> modifiedPages = databaseFile.deleteTuple(tid, t);
    	for (Page page : modifiedPages)
    	{
    		page.markDirty(true, tid);
    		pool.put(page.getId(), page);
    	}
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
    	for (PageId pid: pool.keySet()) {
            flushPage(pid);
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        //get the page to be flushed
    	Page flush = pool.get(pid);
        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        //Write it back into the database
        file.writePage(flush);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        //Get the lease recently used page's ID
    	PageId lru = null; 
        int i = -1;
        //Find the page with the highest priority(least recently used)
        for(PageId iterator : recent.keySet()){
                int priority= recent.get(iterator);
                if( priority> i){
                i = priority;
                lru = iterator;
            }
        }
        //writing the page to disk if it's dirty
        try{
            flushPage(lru);
        }catch (IOException e){
            e.printStackTrace();
        }
        // removing the pageid from recently accessed pages
        recent.remove(lru);
        // remove the page from the bufferpool
        pool.remove(lru);
        
      }

}
