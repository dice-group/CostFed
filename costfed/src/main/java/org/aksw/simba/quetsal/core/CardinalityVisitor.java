package org.aksw.simba.quetsal.core;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.AbstractQueryModelVisitor;

import com.fluidops.fedx.algebra.ExclusiveGroup;
import com.fluidops.fedx.algebra.ExclusiveStatement;
import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.optimizer.OptimizerUtil;

public class CardinalityVisitor extends AbstractQueryModelVisitor<RuntimeException>
{
	protected static int ESTIMATION_TYPE = 0;
	
	public class NodeDescriptor {
		public long card = Long.MAX_VALUE;
		public double sel = 0;
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
	public void meet(Filter filter)  {
		filter.getArg().visit(this);
	}
	
	public void meet(ExclusiveGroup eg)  {
		List<ExclusiveStatement> slst = new LinkedList<ExclusiveStatement>(eg.getStatements()); // make copy
		List<StatementSource> stmtSrces = slst.get(0).getStatementSources();
		assert (stmtSrces.size() == 1);
		long total = Cardinality.getTotalTripleCount(stmtSrces);
		
		ExclusiveStatement leftArg = slst.get(0);
		slst.remove(0);
		
		Set<String> joinVars = new HashSet<String>();
		joinVars.addAll(OptimizerUtil.getFreeVars(leftArg));
		
		long leftCard = Cardinality.getTriplePatternCardinality(leftArg, stmtSrces);
		double leftSelectivity = leftCard/(double)total;
		
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
			long rightCard = Cardinality.getTriplePatternCardinality(rightArg, stmtSrces);
			
			joinVars.addAll(OptimizerUtil.getFreeVars(rightArg));
			
			double sel = 1;
			
			if (ESTIMATION_TYPE == 0) {
				if (commonvars != null && !commonvars.isEmpty()) {
					leftCard = (long)Math.ceil((Math.min(leftCard, rightCard) / 2));
				} else {
					leftCard = (long)Math.ceil(leftCard * rightCard);
				}
			} else if (ESTIMATION_TYPE == 1) {
				double rightSelectivity = rightCard/(double)total;
				if (commonvars != null && !commonvars.isEmpty()) {
					sel = leftSelectivity * rightSelectivity;
				}
				leftCard = (long)Math.ceil(leftCard * rightCard * sel);
				leftSelectivity = sel;
			} else {
				double rightSelectivity = rightCard/(double)total;
				if (commonvars != null && !commonvars.isEmpty()) {
					sel *= Math.min(leftSelectivity, rightSelectivity);
				}
				leftCard = (long)Math.ceil(leftCard * rightCard * sel);
				leftSelectivity = sel;
			}
		}
		current.card = leftCard;
		current.sel = leftSelectivity;
	}
}
