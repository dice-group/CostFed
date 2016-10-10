package org.aksw.simba.quetsal.core;

import java.util.List;
import java.util.concurrent.Callable;

import org.aksw.simba.quetsal.core.algebra.BindJoin;
import org.aksw.simba.quetsal.core.algebra.HashJoin;
import org.aksw.simba.quetsal.core.algebra.JoinRestarter;
import org.aksw.simba.quetsal.core.algebra.TopKSourceStatementPattern;
import org.aksw.simba.quetsal.core.evaluation.BindJoinImpl;
import org.aksw.simba.quetsal.core.evaluation.HashJoinImpl;
import org.aksw.simba.quetsal.core.evaluation.TopKSourceStatementIteration;
import org.aksw.simba.quetsal.core.evaluation.RestarterIteration;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.log4j.Logger;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import com.fluidops.fedx.FederationManager;
import com.fluidops.fedx.algebra.CheckStatementPattern;
import com.fluidops.fedx.algebra.StatementTupleExpr;
import com.fluidops.fedx.evaluation.FederationEvalStrategy;
import com.fluidops.fedx.evaluation.SparqlFederationEvalStrategy;
import com.fluidops.fedx.evaluation.iterator.QueueIteration;
import com.fluidops.fedx.evaluation.iterator.RestartableCloseableIteration;
import com.fluidops.fedx.evaluation.join.ParallelBoundJoinTask;
import com.fluidops.fedx.evaluation.join.ParallelCheckJoinTask;
import com.fluidops.fedx.structures.QueryInfo;

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
		} else if (expr instanceof JoinRestarter) {
			return evaluateJoinRestarter((JoinRestarter)expr, bindings);
		} else if (expr instanceof TopKSourceStatementPattern) {
			return evaluateTopKSourceStatement((TopKSourceStatementPattern)expr, bindings);
		}
		return super.evaluate(expr, bindings);
	}
	
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(TupleExpr expr, List<BindingSet> bindings)
	{
		if (bindings == null || bindings.isEmpty()) {
			return evaluate(expr, (BindingSet)null);
		} else if (bindings.size() == 1) {
			return evaluate(expr, bindings.get(0));
		} else if (expr instanceof StatementTupleExpr) {
			StatementTupleExpr stmt = (StatementTupleExpr)expr;
			return stmt.evaluate(bindings);
		} else {
			throw new NotImplementedException(expr + "");
		}
	}
	
	public void evaluate(QueueIteration<BindingSet> qit, TupleExpr expr, List<BindingSet> bindings)
	{
		Callable<CloseableIteration<BindingSet, QueryEvaluationException>> task = new AsyncEval(expr, bindings);
		FederationManager.getInstance().getScheduler().schedule(qit.createTask(task));
	}
	
	CloseableIteration<BindingSet, QueryEvaluationException> evaluateJoinRestarter(JoinRestarter jri, BindingSet bindings)
	{
		return new RestarterIteration<BindingSet>(
			jri.getQueryInfo(),
			(RestartableCloseableIteration<BindingSet>)evaluate(jri.getArg(0), bindings),
			jri
		);
	}
	
	CloseableIteration<BindingSet, QueryEvaluationException> evaluateTopKSourceStatement(TopKSourceStatementPattern lnu, BindingSet bindings)
	{
		return new TopKSourceStatementIteration(lnu, bindings);
	}
	
	CloseableIteration<BindingSet, QueryEvaluationException> evaluateBindJoin(BindJoin join, BindingSet bindings)
	{
		return new BindJoinImpl(FederationManager.getInstance().getScheduler(), this, join.getLeftArg(), join.getRightArg(), bindings, join.getQueryInfo());
		//return executeJoin(FederationManager.getInstance().getScheduler(), leftResult, join.getRightArg(), bindings, join.getQueryInfo());
	}
	
	CloseableIteration<BindingSet, QueryEvaluationException> evaluateHashJoin(HashJoin join, BindingSet bindings)
	{
		return new HashJoinImpl(FederationManager.getInstance().getScheduler(),	this,
			join.getLeftArg().getBindingNames(), join.getLeftArg(),
			join.getRightArg().getBindingNames(), join.getRightArg(),
			bindings, join.getQueryInfo());
	}
	
	
	class AsyncEval implements Callable<CloseableIteration<BindingSet, QueryEvaluationException>> {
		final TupleExpr expr;
		final BindingSet binding;
		final List<BindingSet> bindings;
		
		public AsyncEval(TupleExpr expr, BindingSet binding) {
			this.expr = expr;
			this.binding = binding;
			this.bindings = null;
		}
		
		public AsyncEval(TupleExpr expr, List<BindingSet> bindings) {
			this.expr = expr;
			this.binding = null;
			this.bindings = bindings;
		}
		
		@Override
		public CloseableIteration<BindingSet, QueryEvaluationException> call() throws Exception {
			if (binding != null) {
				return evaluate(expr, binding);
			} else {
				return evaluate(expr, bindings);
			}
		}
	}
}
