package simpledb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class HeapFileDbIterator implements DbFileIterator {
	
	private final HeapFile heapF;
	private final TransactionId tid;
	
	private int curPnum;
	private Iterator<Tuple> pageIter;
	
	public HeapFileDbIterator(HeapFile heapF, TransactionId tid) {
		// TODO Auto-generated constructor stub
		this.heapF = heapF;
		this.tid = tid;
	}
	private ArrayList<Tuple> getPage(int curPnum) throws TransactionAbortedException, DbException{
		HeapPage hp = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(heapF.getId(), curPnum), Permissions.READ_ONLY);
		ArrayList<Tuple> lt = new ArrayList<Tuple>();
		Iterator<Tuple> it = hp.iterator();
		while(it.hasNext()) {
			lt.add(it.next());
		}
		return lt;
	}
	
	@Override
	public void open() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
		curPnum = 0;
		if(curPnum >= heapF.numPages() || curPnum < 0) {
			throw new NoSuchElementException();
		}
		
		pageIter = getPage(curPnum).iterator();
		
	}

	@Override
	public boolean hasNext() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
		if(pageIter == null) {
			return false;
		}
		while(!pageIter.hasNext()) {
			curPnum++;
			if(curPnum >= heapF.numPages() || curPnum < 0) {
				return false;
			}
			
			
			pageIter = getPage(curPnum).iterator();
		}
		return true;
	}

	@Override
	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
		// TODO Auto-generated method stub
		if(!this.hasNext()) {
			throw new NoSuchElementException();
		}
		
		return pageIter.next();
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
		open();
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		pageIter = null;
	}

}
