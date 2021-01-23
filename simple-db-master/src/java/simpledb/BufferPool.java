package simpledb;

import java.io.*;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;



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
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    public static int deMax_page;
    protected static ConcurrentHashMap<PageId, Page> iDpage;
    
     LockManager lock;
     Object lockT;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
    	iDpage = new ConcurrentHashMap<>();
    	deMax_page = numPages;
    	lock = new LockManager();
    	lockT = new Object();
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
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
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
    
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
    	
    	while(!lock.putkey(tid, pid, perm)) {
    		
    	}
		synchronized (lockT) {
			
		
    	if(iDpage.containsKey(pid)) {
    		return iDpage.get(pid);
    	}

    	if(iDpage.size()>= deMax_page  ) {
    			evictPage();}

    	Page pg = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
		iDpage.put(pid, pg);
		
		return pg;
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
    	lock.replease(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	lock.releaseLock(tid);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2    
    	return lock.hasLock(tid, p);

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
    	synchronized (lockT) {
			Set<PageId> list = lock.getpageid(tid);
			if(list != null) {
				for(PageId p: list) {
					if(!iDpage.containsKey(p)) {
						continue;
					}
						Page pp = iDpage.get(p);
						if(commit) {
							flushPage(p);
							pp.setBeforeImage();
						}else {
							iDpage.remove(p);
							DbFile df = Database.getCatalog().getDatabaseFile(p.getTableId());
							Page pp2 = df.readPage(p);							
							iDpage.put(p, pp2);
						}	
				}
			}
			
    	}
    	transactionComplete(tid);
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
    	//get data file
    	
    		DbFile f = Database.getCatalog().getDatabaseFile(tableId);
    		ArrayList<Page> plist = f.insertTuple(tid, t);
        	for(Page p: plist) {
        		
        			while(iDpage.size() >= deMax_page) {
        				evictPage();
        			}

        		iDpage.put(p.getId(), p);
        		p.markDirty(true, tid);
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
    	
    		DbFile f = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
    		ArrayList<Page> plist = f.deleteTuple(tid, t);
    		for(Page p : plist) {
    			
    				while(iDpage.size() >= deMax_page) {
    					evictPage();
    				}
    		
    			iDpage.put(p.getId(), p);
    			p.markDirty(true, tid);
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
    	synchronized (lockT) {
			
		
    		try {
    			for(PageId idx: iDpage.keySet()) {
    				flushPage(idx);
    			}
    		} catch (Exception e) {
    			// TODO: handle exception
    			e.getMessage();
    			e.getStackTrace();
    		}
		
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
    	iDpage.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
    	Page p = iDpage.get(pid);
    	TransactionId dirty = p.isDirty();
    	if(dirty != null) {
    		// append an update record to the log, with
    	    // a before-image and after-image.

    		Database.getLogFile().logWrite(dirty, p.getBeforeImage(), p);
    		Database.getLogFile().force();
    		Database.getCatalog().getDatabaseFile(p.getId().getTableId()).writePage(p);
    		
    		// use current page contents as the before-image
    		// for the next transaction that modifies this page.
    		//p.setBeforeImage();

    	}
    	
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	for(PageId p: iDpage.keySet()) {
    		Page pp = iDpage.get(p);
    		if(pp.isDirty() == tid) {
    			flushPage(p);
    		}
    	}
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
    	PageId pidi = null;
    	boolean checknulldir = true;
    	for(PageId pidx: iDpage.keySet()) {
    		Page pn = iDpage.get(pidx);
    		if(pn.isDirty() == null) {
    			pidi = pidx;
    			checknulldir = false;
    			break;
    		}
    		
    	}
    	if(checknulldir) {

    	throw new DbException("Bufferpool can not updete dirty pages!");
    	}else {
    		try {
				flushPage(pidi);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		iDpage.remove(pidi);

    	}
    	
    	
    }

}
