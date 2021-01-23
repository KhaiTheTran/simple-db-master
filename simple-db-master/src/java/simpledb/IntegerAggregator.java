package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private int afield;
    private Op what;
    private  Map<Field, Integer> gbA;
    private  Map<Field, Integer> gbC;
    private Type gbfieldtype;
    private boolean swhich;
    private String fnam;
  
    
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	gbA = new HashMap<Field, Integer>();
    	gbC = new HashMap<Field, Integer>();
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.what = what;
    	swhich = false;
    	if(gbfield != Aggregator.NO_GROUPING) {
    		swhich = true;}
    	fnam = "";
    	
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	Field f;
    	if(gbfield == Aggregator.NO_GROUPING) {
    		f = new IntField(0);
    		
    	}else {
    		f = tup.getField(gbfield);
    		fnam = tup.getTupleDesc().getFieldName(gbfield);
    	}
    	
    	IntField aggInF = (IntField)tup.getField(afield); 
    	int status = 1;
    	if(what == Op.SC_AVG) {
    		status = ((IntField)tup.getField(afield+1)).getValue();
    	}
    			
    	if(!gbC.containsKey(f)) {
    		gbC.put(f, status);
    		gbA.put(f, aggInF.getValue());
    	}
    	else {
    		gbC.put(f, gbC.get(f)+status);
    		Integer cval = gbA.get(f), aval = aggInF.getValue();		
    		switch (what) {
            case AVG:
            
            case SUM:
                gbA.put(f, cval + aval);
                break;
            case MAX:
                gbA.put(f, Math.max(cval, aval));
                break;
            case MIN:
                gbA.put(f, Math.min(cval, aval));
                break;
			default:
				break;
        }
    		
    	}
    	
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
    	
        ArrayList<Tuple> tulist = new ArrayList<Tuple>();
        
        	for(Field f: gbC.keySet()) {
        		Tuple tp = new Tuple(gettupleDesc());
        		if(swhich) {
        			tp.setField(0, f);
        		}
        			
            	tp.setField(swhich ? 1:0, new IntField(getCompute(f)[0]));
            	if(what == Op.SUM_COUNT) {
            		tp.setField(swhich ? 2:1, new IntField(getCompute(f)[1]));
            	}
            	tulist.add(tp);
        	}
        		
        	return new TupleIterator(gettupleDesc(), tulist);
        
    }
    /**
     * Get data with the key field from hashMap
     * @param f
     * @return Integer array
     */
    public Integer[] getCompute(Field f) {
    	switch (what) {
        case AVG:
        case SC_AVG:
            return new Integer[]{gbA.get(f) / gbC.get(f)};
        case COUNT:
            return new Integer[]{gbC.get(f)};
        case MAX:
        case MIN:
        case SUM:
            return new Integer[]{gbA.get(f)};
        case SUM_COUNT:
            return new Integer[]{gbA.get(f), gbC.get(f)};
        default:
            break;
    }
    return new Integer[]{};
    }
    /**
     * Make a tuple with according grouping
     * @return Tupledesc
     */
    public TupleDesc gettupleDesc() {
    	TupleDesc tupd;
    	if(swhich) {
    		if(what == Op.SUM_COUNT) {
    			Type[] t = new Type[] {gbfieldtype, Type.INT_TYPE, Type.INT_TYPE};
    			String[] str = new String[] {fnam, "sum", "count"};
    			tupd = new TupleDesc(t, str);
    		}else {
    			Type[] tn = new Type[] {gbfieldtype, Type.INT_TYPE};
    			tupd = new TupleDesc(tn, new String[] {fnam, what.toString()});
    		}
    	}else {
    		if(what == Op.SUM_COUNT) {
    			Type[] t = new Type[] {Type.INT_TYPE, Type.INT_TYPE};
    			String[] str = new String[] {"sum", "count"};
    			tupd = new TupleDesc(t, str);
    		}else {
    			tupd = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {what.toString()});
    		}
    	}
    	return tupd;
    }
}

