package org.aksw.simba.quetsal.core;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.AbstractQueryModelVisitor;

import com.fluidops.fedx.algebra.ExclusiveGroup;
import com.fluidops.fedx.algebra.ExclusiveStatement;
import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.optimizer.OptimizerUtil;
import com.fluidops.fedx.structures.QueryInfo;

public class CardinalityVisitor extends AbstractQueryModelVisitor<RuntimeException>
{
	protected static int ESTIMATION_TYPE = 0;
	final QueryInfo queryInfo;
	
	public CardinalityVisitor(QueryInfo queryInfo) {
		this.queryInfo = queryInfo;
	}
	
	public static class NodeDescriptor {
		public long card = Long.MAX_VALUE;
		public double sel = 0;
		public double mvobjkoef = 1.0;
		public double mvsbjkoef = 1.0;
	}
	
	public static class CardPair {
		public TupleExpr expr;
		public NodeDescriptor nd;
		
		public CardPair(TupleExpr te, NodeDescriptor nd) {
			this.expr = te;
			this.nd = nd;
		}

		@Override
		public String toString() {
			return String.format("CardPair [expr=%s, card=%s, mvs=%s, mvo=%s", expr, nd.card, nd.mvsbjkoef, nd.mvobjkoef);
		}
	}
	
	public static NodeDescriptor getJoinCardinality(Collection<String> commonvars, CardPair left, CardPair right)
	{
		NodeDescriptor result = new NodeDescriptor();
		if (CardinalityVisitor.ESTIMATION_TYPE == 0) {
			if (commonvars != null && !commonvars.isEmpty()) {
				result.card = (long)Math.ceil((Math.min(left.nd.card, right.nd.card)));
				
				// multivalue fixes
				if (left.expr instanceof StatementPattern) {
					StatementPattern leftsp = (StatementPattern)left.expr;
					if (commonvars.contains(leftsp.getSubjectVar().getName())) {
						result.card *= left.nd.mvsbjkoef;
					}
					if (commonvars.contains(leftsp.getObjectVar().getName())) {
						result.card *= left.nd.mvobjkoef;
					}
				}
				if (right.expr instanceof StatementPattern) {
					StatementPattern rightsp = (StatementPattern)right.expr;
					if (commonvars.contains(rightsp.getSubjectVar().getName())) {
						result.card *= right.nd.mvsbjkoef;
					}
					if (commonvars.contains(rightsp.getObjectVar().getName())) {
						result.card *= right.nd.mvobjkoef;
					}
				}
			} else {
				result.card = (long)Math.ceil(left.nd.card * right.nd.card);
			}
		} else {
			result.sel = 1;
			if (commonvars != null && !commonvars.isEmpty()) {
				result.sel *= Math.min(left.nd.sel, right.nd.sel);
			}
			result.card = (long)Math.ceil(left.nd.card * right.nd.card * result.sel);
		}
		return result;
	}
	
	NodeDescriptor current = new NodeDescriptor();
	
	public void reset() {
		current = new NodeDescriptor();
	}
	
	public NodeDescriptor getDescriptor() {
		return current;
	}
	
	public void setDescriptor(NodeDescriptor d) {
		current = d;
	}
	
	public long getCardinality() {
		return current.card;
	}
	
	public static Collection<String> getCommonVars(Collection<String> vars, TupleExpr tupleExpr) {
		Collection<String> commonvars = null;
		Collection<String> exprVars = OptimizerUtil.getFreeVars(tupleExpr);
		for (String argvar : exprVars) {
			if (vars.contains(argvar)) {
				if (commonvars == null) {
					commonvars = new HashSet<String>();
				}
				commonvars.add(argvar);
			}
		}
		return commonvars;
	}
	
	@Override
	public void meet(StatementPattern stmt) {
		List<StatementSource> stmtSrces = queryInfo.getSourceSelection().getStmtToSources().get(stmt);
		current.card = Cardinality.getTriplePatternCardinality(stmt, stmtSrces);
		assert(current.card != 0);
		current.sel = current.card/(double)Cardinality.getTotalTripleCount(stmtSrces);
		current.mvsbjkoef = Cardinality.getTriplePatternSubjectMVKoef(stmt, stmtSrces);
		current.mvobjkoef = Cardinality.getTriplePatternObjectMVKoef(stmt, stmtSrces);
	}
	
	@Override
	public void meet(Filter filter)  {
		filter.getArg().visit(this);
	}
	
	public void meet(ExclusiveGroup eg)  {
		List<ExclusiveStatement> slst = new LinkedList<ExclusiveStatement>(eg.getStatements()); // make copy
		List<StatementSource> stmtSrces = slst.get(0).getStatementSources();
		assert (stmtSrces.size() == 1);
		
		//long total = Cardinality.getTotalTripleCount(stmtSrces);
		
		ExclusiveStatement leftArg = slst.get(0);
		slst.remove(0);
		
		Set<String> joinVars = new HashSet<String>();
		joinVars.addAll(OptimizerUtil.getFreeVars(leftArg));
		
		//long leftCard = Cardinality.getTriplePatternCardinality(leftArg, stmtSrces);
		leftArg.visit(this);
		CardPair left  = new CardPair(leftArg, getDescriptor());
		reset();
		//double leftSelectivity = leftCard/(double)total;
		
		// find possible join order
		while (!slst.isEmpty()) {
			ExclusiveStatement rightArg = null;
			Collection<String> commonvars = null;
			for (int i = 0, n = slst.size(); i < n; ++i) {
				ExclusiveStatement arg = slst.get(i);
				commonvars = getCommonVars(joinVars, arg);
				if (commonvars == null || commonvars.isEmpty()) continue;
				rightArg = arg;
				slst.remove(i);
				break;
			}
			if (rightArg == null) {
				rightArg = slst.get(0);
				slst.remove(0);
			}
			rightArg.visit(this);
			CardPair right = new CardPair(rightArg, getDescriptor());
			reset();
			//long rightCard = Cardinality.getTriplePatternCardinality(rightArg, stmtSrces);
			
			joinVars.addAll(OptimizerUtil.getFreeVars(rightArg));
			
			//double sel = 1;
			
			left.nd = getJoinCardinality(commonvars, left, right);

			//if (ESTIMATION_TYPE == 0) {
				//if (commonvars != null && !commonvars.isEmpty()) {
				//	leftCard = (long)Math.ceil((Math.min(leftCard, rightCard))/2);
				//} else {
				//	leftCard = (long)Math.ceil(leftCard * rightCard);
				//}
			//} else {
			//	double rightSelectivity = rightCard/(double)total;
			//	if (commonvars != null && !commonvars.isEmpty()) {
			//		sel *= Math.min(leftSelectivity, rightSelectivity);
			//	}
			//	leftCard = (long)Math.ceil(leftCard * rightCard * sel);
			//	leftSelectivity = sel;
			//}
		}
		current = left.nd;
		//current.sel = leftSelectivity;
	}
}
