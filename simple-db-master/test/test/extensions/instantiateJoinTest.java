package test.extensions;

import simpledb.DbException;
import simpledb.JoinOptimizer;
import simpledb.LogicalJoinNode;

import simpledb.OpIterator;
import simpledb.ParsingException;
import simpledb.TransactionAbortedException;
import simpledb.systemtest.QueryTest;

import java.io.IOException;
import java.util.Vector;



import org.junit.Before;
import org.junit.Test;


public class instantiateJoinTest {
	
	@Before
	public void setUp() throws Exception {
	}

	@Test(expected = NullPointerException.class)
	public void test() throws ParsingException, NullPointerException, IOException, DbException, TransactionAbortedException {
		
		QueryTest q = new QueryTest();
		Vector<LogicalJoinNode> joins = new Vector<LogicalJoinNode>();
		LogicalJoinNode ln = new LogicalJoinNode();
		joins.add(ln);
		
		OpIterator plan1 = null;
		OpIterator plan2 = null;
		q.queryTest();
		 JoinOptimizer.instantiateJoin(joins.get(0), plan1, plan2);
	}

}
