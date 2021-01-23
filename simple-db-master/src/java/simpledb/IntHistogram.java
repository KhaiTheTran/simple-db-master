package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	private int buckets;
	private int min;
	private int max;
	private int[] hist;
	private int b_size;
	private int ntups;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    	this.buckets = buckets;
    	this.min = min;
    	this.max = max;
    	hist = new int[buckets];
    	ntups = 0;
    	if(buckets > (max-min+1) ) {
    		b_size = max-min+1;
    	}else {
    		b_size = (max-min+1)/buckets;
    	}
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
    	int index = (v-min)/b_size;
    	if(index >= buckets) {
    		index = buckets-1;
    	}
    	hist[index]++;
    	ntups++;
    }

    
    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
    	if(v > max) {
    		if(Predicate.Op.EQUALS.equals(op) || Predicate.Op.GREATER_THAN.equals(op) || Predicate.Op.GREATER_THAN_OR_EQ.equals(op)) {
    			return 0.0;
    		}else{
    			return 1.0;
    		}
    	}
    	if(v < min) {
    		if(Predicate.Op.EQUALS.equals(op) || Predicate.Op.LESS_THAN_OR_EQ.equals(op) || Predicate.Op.LESS_THAN.equals(op)) {
    			return 0.0;
    		}else{
    			return 1.0;
    		}
    	}
    	
      int b_in = (v-min)/b_size;
      if(b_in >= buckets) {
    	  b_in = buckets -1;
      }
      int h_b = hist[b_in];
      double b_f = (h_b*1.0)/ntups;
      if(Predicate.Op.EQUALS.equals(op)) {
    	  return (h_b*1.0/b_size)/ntups;
      }
      if(Predicate.Op.GREATER_THAN.equals(op)|| Predicate.Op.GREATER_THAN_OR_EQ.equals(op)) {
    	  int b_r;
    	  if(b_in == b_size-1) {
    		  b_r = max;
    	  }else {
    		  b_r = min +(b_in+1)*b_size -1;
    	  }
    	  double b_part;
    	  if(Predicate.Op.GREATER_THAN.equals(op)) {
    		  b_part = (b_r - v)*1.0/ b_size;
    	  }else {
    		  b_part = (b_r -v +1)*1.0/b_size;
    	  }
    	  double b = b_part*b_f;
    	  int i;
    	  for(i = b_in + 1 ; i < buckets; i++) {
    		  b += hist[i]*1.0/ntups;
    	  }
    	  return b;
      }
      if(Predicate.Op.LESS_THAN.equals(op) || Predicate.Op.LESS_THAN_OR_EQ.equals(op)) {
    	  int b_l = min + b_in*b_size;
    	  double b_part;
    	  if(Predicate.Op.LESS_THAN.equals(op)) {
    		  b_part = (v-b_l)*1.0/b_size;
    	  }else {
    		  b_part = (v-b_l +1)*1.0/b_size;
    	  }
    	  double b = b_f*b_part;
    	  int i;
    	  for(i = 0 ; i < b_in; i++) {
    		  b += hist[i]*1.0/ntups;
    	  }
    	  return b;
      }
      else {
    	  return 1- (h_b*1.0/b_size)/ntups;
      }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 0.5;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
