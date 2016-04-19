package org.aksw.simba.quetsal.core;

import org.aksw.simba.quetsal.core.algebra.BindJoin;
import org.aksw.simba.quetsal.core.algebra.HashJoin;
import org.aksw.simba.quetsal.core.evaluation.HashJoinImpl;
import org.apache.log4j.Logger;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;

import com.fluidops.fedx.FederationManager;
import com.fluidops.fedx.evaluation.SparqlFederationEvalStrategy;

import info.aduna.iteration.CloseableIteration;

public class TBSSFederationEvalStrategy extends SparqlFederationEvalStrategy {
	static Logger log = Logger.getLogger(TBSSFederationEvalStrategy.class);
			
	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(TupleExpr expr, BindingSet bindings)
	{
		if (expr instanceof BindJoin) {
			return evaluateBindJoin((BindJoin)expr, bindings);
		} else if (expr instanceof HashJoin) {
			return evaluateHashJoin((HashJoin)expr, bindings);
		}
		return super.evaluate(expr, bindings);
	}
	
	CloseableIteration<BindingSet, QueryEvaluationException> evaluateBindJoin(BindJoin join, BindingSet bindings)
	{
		CloseableIteration<BindingSet, QueryEvaluationException> leftResult = evaluate(join.getLeftArg(), bindings);
		return executeJoin(FederationManager.getInstance().getScheduler(), leftResult, join.getRightArg(), bindings, join.getQueryInfo());
	}
	
	CloseableIteration<BindingSet, QueryEvaluationException> evaluateHashJoin(HashJoin join, BindingSet bindings)
	{
		return new HashJoinImpl(FederationManager.getInstance().getScheduler(),
			join.getLeftArg().getBindingNames(), evaluate(join.getLeftArg(), bindings),
			join.getRightArg().getBindingNames(), evaluate(join.getRightArg(), bindings),
			bindings, join.getQueryInfo());
	}
}
