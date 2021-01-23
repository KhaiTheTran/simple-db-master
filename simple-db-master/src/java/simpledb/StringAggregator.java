package simpledb;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private ConcurrentHashMap<Field, Integer> gbA;
    private boolean swhich;
   
    private String fnam, gbfnam;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * 
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * 
     * @param afield the 0-based index of the aggregate field in the tuple
     * 
     * @param what aggregation operator to use -- only supports COUNT
     * 
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	if(what != Op.COUNT) {
    		throw new IllegalArgumentException();
    	}
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.what = what;
    	swhich = false;
    	if(gbfield == Aggregator.NO_GROUPING) {
        	swhich = true;}
    	gbA = new ConcurrentHashMap<>();
    	
    	fnam = "";
    	gbfnam = "";
    }
    public StringAggregator() {
    	
    }
    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	
    	Field f;
    	int curcount;
    	
    	if(swhich) {
    		f = new IntField(Aggregator.NO_GROUPING);
    		
    		
    	}else {
    		f = tup.getField(gbfield);
    		gbfnam = tup.getTupleDesc().getFieldName(gbfield);
    		
    	}
    	if(gbA.containsKey(f)) {
    		
    		gbA.put(f, gbA.get(f));
    		
    	}else {
    				
    			gbA.put(f, 0);
    			
    	}
    	curcount = gbA.get(f);
    	++curcount;
    	gbA.put(f, curcount);
    	fnam = tup.getTupleDesc().getFieldName(afield);
    	
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
    	ArrayList<Tuple> tulist = new ArrayList<>();
        if(swhich) {
        	for(Field f: gbA.keySet()) {
        		int keys = gbA.get(f);
        		
        		Tuple tp = new Tuple(gettupleDesc(swhich,fnam,gbfieldtype,gbfnam));
        		tp.setField(1, new IntField(keys));
        		tulist.add(tp);
        		
        	}
        }else {
        	for (Field f: gbA.keySet()) {
        		int keys = gbA.get(f);
        		
        		Tuple tp = new Tuple(gettupleDesc(swhich,fnam,gbfieldtype,gbfnam));
        		tp.setField(0, f);
        		tp.setField(1, new IntField(keys));
        		tulist.add(tp);
        		
        	}
        }
        return new TupleIterator(gettupleDesc(swhich,fnam,gbfieldtype,gbfnam), tulist);
    }
    public TupleDesc gettupleDesc(boolean swhich1, String fnam1, Type gbfieldtype1,String gbfnam1) {
    	Type[] ftype;
    	String[] fnaml;
    	if(swhich1) {
    		ftype = new Type[1];
    		fnaml = new String[1];
    		ftype[0] = Type.INT_TYPE;
    		fnaml[0] = fnam1;
    	}else {
    		ftype = new Type[2];
    		fnaml = new String[2];
    		ftype[0] = gbfieldtype1;
    		fnaml[0] = gbfnam1;
    		ftype[1] = Type.INT_TYPE;
    		fnaml[1] = fnam1;
    	}	 
    	return new TupleDesc(ftype, fnaml);
    }
}
