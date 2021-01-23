package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private TupleDesc td1;
    
    private ArrayList<Field> fiel;
    private RecordId rdID;
    
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
    	td1 = td;
    	fiel = new ArrayList<>(td.numFields());
    	int i;
    	for(i=0; i < td.numFields();++i) {
    		fiel.add(null);
    	}
    	
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td1;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return rdID;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
    	rdID = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
    	if(td1.numFields() > i) fiel.set(i, f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
    	if(i < fiel.size() && i > -1) {
    		return fiel.get(i);}
    	//System.out.println(i);
    	throw new IllegalArgumentException("Input error!");
    	
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
    	String tab = "";
    	int i;
    	for (i= 0; i < td1.numFields(); i++) {
    		if((td1.numFields() -1) == i) {
    			tab += fiel.get(i)+"\n";
    		}else {
    			tab += fiel.get(i)+"\t";
    		}
			
		}
    	return tab;
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
    	 
        return fiel.iterator();
        
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
    	td1 = td;
    	fiel = new ArrayList<>(td1.numFields());
    }
}
