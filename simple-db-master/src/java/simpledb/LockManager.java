/**
 * 
 */
package simpledb;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;



/**
 * @author Khai Tran
 *
 */
public class LockManager {
 
    private  ConcurrentHashMap<PageId, Permissions> paper ;
    private  Map<TransactionId, Set<PageId>> lockTran;
    private  Map<PageId, Set<TransactionId>> lockR;
    private ConcurrentHashMap<TransactionId, PageId> lockRW;
   
    /**
     * @param tid the transaction on whose behalf we want to acquire the lock
     * @param pid the page over which we want to acquire the lock
     * @param perm the desired lock permissions
     */
    public LockManager() {
        
       
        this.paper = new ConcurrentHashMap<PageId, Permissions>();
        this.lockRW = new ConcurrentHashMap<TransactionId, PageId>();
        this.lockR = new HashMap<PageId, Set<TransactionId>>();
        this.lockTran = new HashMap<TransactionId, Set<PageId>>();
    }
    public synchronized void replease(PageId pid, TransactionId tid) {

    		lockTran.get(tid).remove(pid);
    		if(lockTran.get(tid).isEmpty()) {
    			lockTran.remove(tid);
    		}
    			
    		if(lockR.containsKey(pid) && lockR.get(pid).contains(tid)) {
    			lockR.get(pid).remove(tid);
    			if(lockR.get(pid).isEmpty()) lockR.remove(pid);
    		}
    	
    	
    }
    /**
     * Manage the lock on pages and transaction, deadlock
     * @param tid
     * @param pid
     * @param perm
     * @return true if all locks in load in page
     * @throws TransactionAbortedException
     */
    public synchronized boolean putkey(TransactionId tid, PageId pid, Permissions perm)throws TransactionAbortedException {
    	
    	if(!lockRW.containsKey(tid)) {
    		lockRW.put(tid, pid);
    		deadlock(tid, pid);
    	}
    	
    	if(!acquired(tid, pid, perm)) {
    		 return false;
    	}
    	
    	if(!lockTran.containsKey(tid)) {
      	  lockTran.put(tid, new HashSet<PageId>());
        }
        lockTran.get(tid).add(pid);
    	
    	if(!lockR.containsKey(pid)) {
    		lockR.put(pid, new HashSet<TransactionId>());
    	}
    	lockR.get(pid).add(tid);
    	paper.put(pid, perm);
    	lockRW.remove(tid);
    	return true;	
    }
    
    /**
     * Check dead lock
     * @param lisT
     * @param tid
     * @throws TransactionAbortedException
     */
    private synchronized void checkdeadlock(Set<TransactionId> lisT, TransactionId tid) throws TransactionAbortedException {
    	if (lisT.contains(tid)) {
            throw new TransactionAbortedException();
        }
        lisT.add(tid);
        for (TransactionId tidd : getTran(tid)) {
            if (!tid.equals(tidd)) {
            	checkdeadlock(lisT, tidd);
            }
        }
    }
    /**
     * Check dead lock
     * @param tid
     * @param pid
     * @throws TransactionAbortedException
     */
    private synchronized void deadlock(TransactionId tid, PageId pid) throws TransactionAbortedException {
    	 if (lockR.containsKey(pid)) {
             for (TransactionId tid1 : lockR.get(pid)) {
                 if (!tid1.equals(tid)) {
                	 checkdeadlock(new HashSet<TransactionId>(), tid1);
                 }
             }
         }
    }
    /**
     * Check dead lock
     * @param tid
     * @return list of transaction
     * @throws TransactionAbortedException
     */
    private synchronized Set<TransactionId> getTran (TransactionId tid) throws TransactionAbortedException {
    	Set<TransactionId> list = new HashSet<TransactionId>();
        if (!lockRW.containsKey(tid)) {
            return list;
        }
        PageId pid = lockRW.get(tid);
        if (lockR.containsKey(pid)) {
            for (TransactionId tiddd : lockR.get(pid)) {
                list.add(tiddd);
            }
        }
        return list;
    }
    /**
     * return a set of page id to complete transaction in bufferpool
     * @param tid
     * @return Set<PageId>
     */
    public synchronized Set<PageId> getpageid (TransactionId tid){
    	return lockTran.get(tid);
    }
    public synchronized boolean hasLock(TransactionId tid, PageId pid) {
    	return lockR.containsKey(pid) && lockR.get(pid).contains(tid) && lockTran.containsKey(tid) && lockTran.get(tid).contains(pid);
    }

    /**
     * @return true if we successfully acquired the specified lock
     */
    public synchronized boolean acquired(TransactionId tid, PageId pid, Permissions per) {
    	
    	if(!paper.containsKey(pid)) {
    		return true;
    	}
    	if(!lockR.containsKey(pid)) {
    		return true;
    	}
    	boolean db = (paper.get(pid).equals((Permissions.READ_ONLY)));
    	boolean dA = (per.equals((Permissions.READ_ONLY)));
    	boolean lot = lockR.containsKey(pid) && lockR.get(pid).size() >= 2;
    	
    	
    	if(db) {
    		if(dA) {
    			return true;
    		}else {
    			return !lot && lockR.get(pid).contains(tid);
    		}
    		
    	}else {
    		 return lockR.get(pid).contains(tid);
    	}
       
    }
    /**
     * Release all locks associated with a given transaction.
     * @param tid
     */
    public synchronized void releaseLock(TransactionId tid) {
    	Set<PageId> pids = new HashSet<PageId>();
        if (lockTran.containsKey(tid)) {
            for (PageId pid : lockTran.get(tid)) {
                pids.add(pid);
            }
        }
        // put to set to eliminate duplicate
        for (PageId pid : pids) {
            replease(pid, tid);
        }
        lockRW.remove(tid);
    }
    /**
     * @return an Exception instance if one occured during lock acquisition;
     *   null otherwise
     */
   
}
