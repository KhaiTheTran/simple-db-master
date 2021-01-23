package test.extensions;
import simpledb.DbException;
import simpledb.JoinOptimizer;
import simpledb.LogicalJoinNode;
import simpledb.LogicalPlan;

import simpledb.ParsingException;

import simpledb.TransactionAbortedException;


import static org.junit.Assert.assertEquals;


import java.io.IOException;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import org.junit.Before;
import org.junit.Test;

public class enumerateSubsetsTest {

	
	@Before
	public void setUp() throws Exception {
	}

	@Test( )
	public void test() throws IOException, DbException, TransactionAbortedException, ParsingException {
		
		LogicalPlan p = new LogicalPlan();
		Vector<LogicalJoinNode> joins = new Vector<LogicalJoinNode>();
		LogicalJoinNode ln = new LogicalJoinNode();
		joins.add(ln);
		JoinOptimizer jo = new JoinOptimizer(p, joins);
		
    	int num_nodes = joins.size();
    	
    	BitSet joinsbs = new BitSet();
    	for (int i = 0; i < num_nodes; i++) {
    		joinsbs.set(i);
    	}
    	
    	for (int i = 1; i <= num_nodes; i++) {
    		Set<BitSet> subsetOfJ = jo.enumerateSubsets(joinsbs, i);
    		Set<BitSet> expected = new HashSet<>();
    		joinsbs.set(0);
    		expected.add(joinsbs);
			assertEquals(expected, subsetOfJ);
    	}
    	
	}

}
