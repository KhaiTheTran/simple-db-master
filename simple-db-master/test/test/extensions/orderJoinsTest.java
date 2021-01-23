package test.extensions;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import org.junit.Before;
import org.junit.Test;

import simpledb.Database;
import simpledb.DbException;
import simpledb.JoinOptimizer;
import simpledb.JoinOptimizerTest;
import simpledb.LogicalJoinNode;
import simpledb.LogicalPlan;
import simpledb.ParsingException;
import simpledb.TransactionAbortedException;

public class orderJoinsTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test(expected = ParsingException.class)
	public void test() throws ParsingException, IOException, DbException, TransactionAbortedException {
		LogicalPlan p = new LogicalPlan();
		Vector<LogicalJoinNode> joins = new Vector<LogicalJoinNode>();
		JoinOptimizerTest jt = new JoinOptimizerTest();
		LogicalJoinNode ln = new LogicalJoinNode();
		joins.add(ln);
		JoinOptimizer jo = new JoinOptimizer(p, joins);
		
    	int num_nodes = joins.size();
    	
    	BitSet joinsbs = new BitSet();
    	for (int i = 0; i < num_nodes; i++) {
    		joinsbs.set(i);
    	}
    	jt.estimateJoinCostTest();
    	jt.bigOrderJoinsTest();
    	jt.estimateJoinCardinality();
    	jt.nonequalityOrderJoinsTest();
    	for (int i = 1; i <= num_nodes; i++) {
    		Set<BitSet> subsetOfJ = jo.enumerateSubsets(joinsbs, i);
    		Set<BitSet> expected = new HashSet<>();
    		joinsbs.set(0);
    		expected.add(joinsbs);
			assertEquals(expected, subsetOfJ);
    	}
    	
    	
	}

}
