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
	private File f;
	private TupleDesc td;
	 
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this.f = f;
    	this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
    	return f.getAbsoluteFile().hashCode();
        
    }

    
    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
    	return td;
       
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
    	int desize = BufferPool.getPageSize();
    	
    	try {
    		// add new page if enough number page
    		if(pid.getPageNumber() == numPages()) {
    			
    			Page id = new HeapPage((HeapPageId)pid, HeapPage.createEmptyPageData());
    			writePage(id);
    			return id;
    		} else {
			RandomAccessFile rafile = new RandomAccessFile(f,"r");
			int size = desize * pid.getPageNumber();
			// initial byte size
			byte[] bt = new byte[desize];
			
			rafile.seek(size);
			// read byte
			rafile.read(bt);
			
			HeapPageId hpid = (HeapPageId)pid;
			rafile.close();
			return new HeapPage(hpid, bt);
    		}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			throw new IllegalArgumentException();
		}
    }

    // see DbFile.java for javadocs
    /**
     * Push the specified page to disk.
     *
     * @param p The page to write.  page.getId().pageno() specifies the offset into the file where the page should be written.
     * @throws IOException if the write fails
     *
     */
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    	int psize = BufferPool.getPageSize();
    	try {
    		// load writer
			RandomAccessFile writefile = new RandomAccessFile(this.f, "rw");
			// initail size page
			int size = psize * page.getId().getPageNumber();
			// set byte array
			byte[] bt = new byte[psize];
			// find free space
			writefile.seek(size);
			// get data from page to byte array
			bt = page.getPageData();
			// write byte array to disk and closed
			writefile.write(bt);
			writefile.close();
		} catch (IOException e) {
			// TODO: handle exception
			e.printStackTrace();
			throw new IllegalArgumentException();
		}
    	
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)Math.ceil(f.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    /**
     * Inserts the specified tuple to the file on behalf of transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @param tid The transaction performing the update
     * @param t The tuple to add.  This tuple should be updated to reflect that
     *          it is now stored in this file.
     * @return An ArrayList contain the pages that were modified
     * @throws DbException if the tuple cannot be added
     * @throws IOException if the needed file can't be read/written
     */
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
      
        // not necessary for lab1
    	int i;
    	//readPage(getId());
    	try {
    		for(i = 0; i< numPages(); i++) {
    			int tableId = getId();
    			PageId pid = new HeapPageId(tableId, i);
    			if(pid.getPageNumber() == numPages()) {
        			
        			Page id = new HeapPage((HeapPageId)pid, HeapPage.createEmptyPageData());
        			writePage(id);}
    			Page p = Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
    			if(((HeapPage)p).getNumEmptySlots()> 0) {
    				HeapPage hp = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
    				hp.insertTuple(t);
    				return new ArrayList<Page>(Arrays.asList(hp));
    			}
				Database.getBufferPool().releasePage(tid, pid);
    		}
    		HeapPageId idp = new HeapPageId(this.getId(), this.numPages());
			HeapPage hp1 = (HeapPage)Database.getBufferPool().getPage(tid, idp, Permissions.READ_WRITE);
			hp1.insertTuple(t);
			return new ArrayList<Page>(Arrays.asList(hp1));
		} catch (DbException e) {
			// TODO: handle exception
			System.out.println(e);
			e.getStackTrace();
			System.out.println(e);
			System.out.println("e needed file can't be read/written!");
			throw new IOException("the tuple cannot be added!");
		}   
    }
 

    // see DbFile.java for javadocs
    /**
     * Removes the specified tuple from the file on behalf of the specified
     * transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @param tid The transaction performing the update
     * @param t The tuple to delete.  This tuple should be updated to reflect that
     *          it is no longer stored on any page.
     * @return An ArrayList contain the pages that were modified
     * @throws DbException if the tuple cannot be deleted or is not a member
     *   of the file
     */
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        
        // not necessary for lab1
    	if(t.getRecordId() == null) {
    		throw new DbException("The tuple cannot be deleted or is not a member of the file!");
    	}
    	
    		PageId pid = t.getRecordId().getPageId();
    		HeapPage hp = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            hp.deleteTuple(t);
            return new ArrayList<Page>(Arrays.asList(hp));

    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
    	
        return new HeapFileDbIterator(this, tid);
    }

}

