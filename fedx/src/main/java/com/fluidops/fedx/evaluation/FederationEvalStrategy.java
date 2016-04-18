/*
 * Copyright (C) 2008-2013, fluid Operations AG
 *
 * FedX is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.fluidops.fedx.evaluation;

import java.util.List;
import java.util.concurrent.Executor;

import org.apache.log4j.Logger;
import org.openrdf.model.IRI;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.BooleanLiteral;
import org.openrdf.model.impl.SimpleValueFactory;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.BinaryTupleOperator;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.openrdf.query.algebra.evaluation.federation.ServiceJoinIterator;
import org.openrdf.query.algebra.evaluation.impl.SimpleEvaluationStrategy;
import org.openrdf.query.algebra.evaluation.iterator.CollectionIteration;
import org.openrdf.query.algebra.evaluation.util.QueryEvaluationUtil;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import com.fluidops.fedx.Config;
import com.fluidops.fedx.EndpointManager;
import com.fluidops.fedx.FederationManager;
import com.fluidops.fedx.algebra.CheckStatementPattern;
import com.fluidops.fedx.algebra.ConjunctiveFilterExpr;
import com.fluidops.fedx.algebra.EmptyResult;
import com.fluidops.fedx.algebra.ExclusiveGroup;
import com.fluidops.fedx.algebra.FedXExpr;
import com.fluidops.fedx.algebra.FedXExprVisitor;
import com.fluidops.fedx.algebra.FedXService;
import com.fluidops.fedx.algebra.FilterExpr;
import com.fluidops.fedx.algebra.IndependentJoinGroup;
import com.fluidops.fedx.algebra.NJoin;
import com.fluidops.fedx.algebra.NUnion;
import com.fluidops.fedx.algebra.ProjectionWithBindings;
import com.fluidops.fedx.algebra.SingleSourceQuery;
import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.algebra.StatementTupleExpr;
import com.fluidops.fedx.cache.Cache;
import com.fluidops.fedx.cache.CacheUtils;
import com.fluidops.fedx.evaluation.concurrent.ControlledWorkerScheduler;
import com.fluidops.fedx.evaluation.concurrent.ParallelServiceExecutor;
import com.fluidops.fedx.evaluation.join.ControlledWorkerBoundJoin;
import com.fluidops.fedx.evaluation.join.ControlledWorkerJoin;
import com.fluidops.fedx.evaluation.join.SynchronousBoundJoin;
import com.fluidops.fedx.evaluation.join.SynchronousJoin;
import com.fluidops.fedx.evaluation.union.ControlledWorkerUnion;
import com.fluidops.fedx.evaluation.union.ParallelGetStatementsTask;
import com.fluidops.fedx.evaluation.union.ParallelPreparedAlgebraUnionTask;
import com.fluidops.fedx.evaluation.union.ParallelPreparedUnionTask;
import com.fluidops.fedx.evaluation.union.ParallelUnionOperatorTask;
import com.fluidops.fedx.evaluation.union.WorkerUnionBase;
import com.fluidops.fedx.exception.FedXRuntimeException;
import com.fluidops.fedx.statistics.Statistics;
import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.structures.QueryInfo;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.EmptyIteration;
import info.aduna.iteration.SingletonIteration;


/**
 * Base class for the Evaluation strategies.
 * 
 * @author Andreas Schwarte
 * 
 * @see SailFederationEvalStrategy
 * @see SparqlFederationEvalStrategy
 *
 */
public abstract class FederationEvalStrategy extends SimpleEvaluationStrategy {
	static Logger log = Logger.getLogger(FederationEvalStrategy.class);
	
	protected Executor executor;
	protected Cache cache;
	protected Statistics statistics;
	
	public FederationEvalStrategy() {
		super(new org.openrdf.query.algebra.evaluation.TripleSource() {

			@Override
			public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(
					Resource subj, IRI pred, Value obj, Resource... contexts)
					throws QueryEvaluationException
			{
				throw new FedXRuntimeException(
						"Federation Strategy does not support org.openrdf.query.algebra.evaluation.TripleSource#getStatements." +
						" If you encounter this exception, please report it.");
			}

			@Override
			public ValueFactory getValueFactory() {
				return SimpleValueFactory.getInstance();
			}}, (FederatedServiceResolver)null);	// !!! ", null" added
		
		this.executor = FederationManager.getInstance().getExecutor();
		this.cache = FederationManager.getInstance().getCache();
		this.statistics = FederationManager.getInstance().getStatistics();
	}

	class EvalVisitor implements FedXExprVisitor {
		CloseableIteration<BindingSet, QueryEvaluationException> result = null;
		BindingSet bindings;
		
		
		public EvalVisitor(BindingSet bindings) {
			this.bindings = bindings;
		}
		
		public CloseableIteration<BindingSet, QueryEvaluationException> get() {
			return result;
		}
		
		@Override
		public void meet(StatementTupleExpr es) {
			result = es.evaluate(bindings);
		}

		@Override
		public void meet(ExclusiveGroup eg) {
			result = eg.evaluate(bindings);
		}
		
		@Override
		public void meet(NJoin nj) {
			result = evaluateNJoin(nj, bindings);
		}
		
		@Override
		public void meet(NUnion nu) {
			result = evaluateNaryUnion(nu, bindings);
		}

		@Override
		public void meet(EmptyResult er) {
			result = new EmptyIteration<BindingSet, QueryEvaluationException>();
		}

		@Override
		public void meet(SingleSourceQuery ssq) {
			result = evaluateSingleSourceQuery(ssq, bindings);
		}

		@Override
		public void meet(FedXService fs) {
			result = evaluateService(fs, bindings);
		}

		@Override
		public void meet(ProjectionWithBindings pwb) {
			result = evaluateProjectionWithBindings(pwb, bindings);
		}

		@Override
		public void meet(IndependentJoinGroup ijg) {
			result = evaluateIndependentJoinGroup(ijg, bindings);	// XXX
		}
		}
	
	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
			TupleExpr expr, BindingSet bindings)
	{
		if (expr instanceof FedXExpr) {
			FedXExpr fexpr = (FedXExpr)expr;
			EvalVisitor visitor = new EvalVisitor(bindings);
			fexpr.visit(visitor);
			return visitor.get();
		}
		
		return super.evaluate(expr, bindings);
	}
	
	
	/**
	 * Retrieve the statements matching the provided subject, predicate and object value from the 
	 * federation members.<p>
	 * 
	 * For a bound statement, i.e. a statement with no free variables, the statement itself is 
	 * returned if some member has this statement, an empty iteration otherwise.<p>
	 * 
	 * If the statement has free variables, i.e. one of the provided arguments in <code>null</code>,
	 * the union of results from relevant statement sources is constructed.
	 * 
	 * @param subj
	 * @param pred
	 * @param obj
	 * @param contexts
	 * @return
	 * 
	 * @throws RepositoryException
	 * @throws MalformedQueryException
	 * @throws QueryEvaluationException
	 */
	public CloseableIteration<Statement, QueryEvaluationException> getStatements(QueryInfo queryInfo, Resource subj, IRI pred, Value obj, Resource... contexts)
	{

		if (contexts.length != 0)
			log.warn("Context queries are not yet supported by FedX.");
		
		List<Endpoint> members = FederationManager.getInstance().getFederation().getMembers();
		
		// a bound query: if at least one fed member provides results
		// return the statement, otherwise empty result
		if (subj != null && pred != null && obj != null) {
			if (CacheUtils.checkCacheUpdateCache(cache, members, subj, pred, obj)) {
				return new SingletonIteration<Statement, QueryEvaluationException>(SimpleValueFactory.getInstance().createStatement(subj, pred, obj));
			}
			return new EmptyIteration<Statement, QueryEvaluationException>();
		}
		
		// form the union of results from relevant endpoints
		List<StatementSource> sources = CacheUtils.checkCacheForStatementSourcesUpdateCache(cache, members, subj, pred, obj);
		
		if (sources.isEmpty())
			return new EmptyIteration<Statement, QueryEvaluationException>();
		
		if (sources.size() == 1) {
			Endpoint e = EndpointManager.getEndpointManager().getEndpoint(sources.get(0).getEndpointID());
			return e.getTripleSource().getStatements(e.getConn(), subj, pred, obj, contexts);
		}
		
		// TODO why not collect in parallel?
		//WorkerUnionBase<Statement> union = new SynchronousWorkerUnion<Statement>(queryInfo);
		WorkerUnionBase<Statement> union = new ControlledWorkerUnion<Statement>(FederationManager.getInstance().getScheduler(), queryInfo);
		
		for (StatementSource source : sources) {
			Endpoint e = EndpointManager.getEndpointManager().getEndpoint(source.getEndpointID());
			union.addTask(new ParallelGetStatementsTask(e.getTripleSource(), e.getConn(), subj, pred, obj, contexts));
		}
		
		// TODO distinct iteration ?
		
		return union;
	}
	
	
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluateService(FedXService service, BindingSet bindings) {
		//FederationManager.getInstance().getScheduler().schedule(new ParallelServiceTask());
		
		ParallelServiceExecutor pe = new ParallelServiceExecutor(service, this, bindings);
		pe.run();		// non-blocking (blocking happens in the iterator)
		return pe;
	}
	
	
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluateSingleSourceQuery(SingleSourceQuery query, BindingSet bindings) {
		
		try
		{
			Endpoint source = query.getSource();		
			return source.getTripleSource().getStatements(query.getQueryString(), source.getConn(), query.getQueryInfo().getQueryType());
		} catch (RepositoryException e) {
			throw new QueryEvaluationException(e);
		} catch (MalformedQueryException e)	{
			throw new QueryEvaluationException(e);
		}
		
	}
	
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluateNJoin(NJoin join, BindingSet bindings) {
		List<TupleExpr> args = join.getArgs();
		CloseableIteration<BindingSet, QueryEvaluationException> result = evaluate(args.get(0), bindings);
		ControlledWorkerScheduler scheduler = FederationManager.getInstance().getScheduler();
		for (int i = 1, n = args.size(); i < n; ++i) {
			result = executeJoin(scheduler, result, args.get(i), bindings, join.getQueryInfo());
		}
		return result;
	}
	
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluateNaryUnion(NUnion union, BindingSet bindings) {
		
		ControlledWorkerScheduler unionScheduler = FederationManager.getInstance().getScheduler();
		ControlledWorkerUnion<BindingSet> unionRunnable = new ControlledWorkerUnion<BindingSet>(unionScheduler, union.getQueryInfo());
		
		for (int i = 0; i < union.getNumberOfArguments(); i++) {
			unionRunnable.addTask(new ParallelUnionOperatorTask(this, union.getArg(i), bindings));
		}
		
		return unionRunnable;
	}

	/**
	 * Execute the join in a separate thread using some join executor. 
	 * 
	 * Join executors are for instance:
	 * 	- {@link SynchronousJoin}
	 *  - {@link SynchronousBoundJoin}
	 *  - {@link ControlledWorkerJoin}
	 *  - {@link ControlledWorkerBoundJoin}
	 *
	 * For endpoint federation use controlled worker bound join, for local federation use
	 * controlled worker join. The other operators are there for completeness.
	 * 
	 * Use {@link FederationEvalStrategy#executor} to execute the join (it is a runnable).
	 * 
	 * @param joinScheduler
	 * @param leftIter
	 * @param rightArg
	 * @param bindings
	 * @return
	 */
	protected abstract CloseableIteration<BindingSet, QueryEvaluationException> executeJoin(ControlledWorkerScheduler joinScheduler, CloseableIteration<BindingSet, QueryEvaluationException> leftIter, TupleExpr rightArg, BindingSet bindings, QueryInfo queryInfo);
	
	
	
	public abstract CloseableIteration<BindingSet, QueryEvaluationException> evaluateExclusiveGroup(ExclusiveGroup group, RepositoryConnection conn, TripleSource tripleSource, BindingSet bindings);

	
	
	/**
	 * Evaluate a bound join at the relevant endpoint, i.e. i.e. for a group of bindings retrieve
	 * results for the bound statement from the relevant endpoints
	 * 
	 * @param stmt
	 * @param bindings
	 * @return
	 * @throws QueryEvaluationException
	 */
	public abstract CloseableIteration<BindingSet, QueryEvaluationException> evaluateBoundJoinStatementPattern(StatementTupleExpr stmt, final List<BindingSet> bindings) throws QueryEvaluationException;
	
	/**
	 * Perform a grouped check at the relevant endpoints, i.e. for a group of bindings keep only 
	 * those for which at least one endpoint provides a result to the bound statement.
	 * 
	 * @param stmt
	 * @param bindings
	 * @return
	 * @throws QueryEvaluationException
	 */
	public abstract CloseableIteration<BindingSet, QueryEvaluationException> evaluateGroupedCheck(CheckStatementPattern stmt, final List<BindingSet> bindings) throws QueryEvaluationException;
	
	
	
	public abstract CloseableIteration<BindingSet, QueryEvaluationException> evaluateIndependentJoinGroup(IndependentJoinGroup joinGroup, final BindingSet bindings) throws QueryEvaluationException; 
	
	public abstract CloseableIteration<BindingSet, QueryEvaluationException> evaluateIndependentJoinGroup(IndependentJoinGroup joinGroup, final List<BindingSet> bindings) throws QueryEvaluationException;
		
	
	/**
	 * Evaluate a projection containing additional values, e.g. set from a filter expression
	 * 
	 * @return
	 * @throws QueryEvaluationException
	 */
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluateProjectionWithBindings(ProjectionWithBindings projection, BindingSet bindings) throws QueryEvaluationException {
		QueryBindingSet actualBindings = new QueryBindingSet(bindings);
		for (Binding b : projection.getAdditionalBindings())
			actualBindings.addBinding(b);
		return evaluate((Projection)projection, actualBindings);
	}
	
	/**
	 * Evaluate a SERVICE using vectored evaluation, taking the provided bindings as input.
	 * 
	 * See {@link ControlledWorkerBoundJoin} and {@link Config#getEnableServiceAsBoundJoin()}
	 * 
	 * @param service
	 * @param bindings
	 * @return
	 * @throws QueryEvaluationException
	 */
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluateService(FedXService service, final List<BindingSet> bindings) {
		
		return new ServiceJoinIterator(new CollectionIteration<BindingSet, QueryEvaluationException>(bindings), service.getService(), EmptyBindingSet.getInstance(), this);
	}
	
	public Value evaluate(ValueExpr expr, BindingSet bindings) {
		
		if (expr instanceof FilterExpr)
			return evaluate((FilterExpr)expr, bindings);
		if (expr instanceof ConjunctiveFilterExpr)
			return evaluate((ConjunctiveFilterExpr)expr, bindings);
		
		return super.evaluate(expr, bindings);
	}
	
	public Value evaluate(FilterExpr node, BindingSet bindings) {
		
		Value v = evaluate(node.getExpression(), bindings);
		return BooleanLiteral.valueOf(QueryEvaluationUtil.getEffectiveBooleanValue(v));
	}
	
	
	public Value evaluate(ConjunctiveFilterExpr node, BindingSet bindings) {
		
		ValueExprEvaluationException error = null;
		
		for (FilterExpr expr : node.getExpressions()) {
			
			try {
				Value v = evaluate(expr.getExpression(), bindings);
				if (QueryEvaluationUtil.getEffectiveBooleanValue(v) == false) {
					return BooleanLiteral.FALSE;
				}
			} catch (ValueExprEvaluationException e) {
				error = e;
			}
		}
		
		if (error!=null)
			throw error;
		
		return BooleanLiteral.TRUE;
	}
	
	
	
	protected CloseableIteration<BindingSet, QueryEvaluationException> evaluateAtStatementSources(Object preparedQuery, List<StatementSource> statementSources, QueryInfo queryInfo) {
		if (preparedQuery instanceof String)
			return evaluateAtStatementSources((String)preparedQuery, statementSources, queryInfo);
		if (preparedQuery instanceof TupleExpr)
			return evaluateAtStatementSources((TupleExpr)preparedQuery, statementSources, queryInfo);
		throw new RuntimeException("Unsupported type for prepared query: " + preparedQuery.getClass().getCanonicalName());
	}
	
	protected CloseableIteration<BindingSet, QueryEvaluationException> evaluateAtStatementSources(String preparedQuery, List<StatementSource> statementSources, QueryInfo queryInfo) {
		
		try {
			CloseableIteration<BindingSet, QueryEvaluationException> result;
			
			if (statementSources.size()==1) {				
				Endpoint ownedEndpoint = EndpointManager.getEndpointManager().getEndpoint(statementSources.get(0).getEndpointID());
				RepositoryConnection conn = ownedEndpoint.getConn();
				com.fluidops.fedx.evaluation.TripleSource t = ownedEndpoint.getTripleSource();
				result = t.getStatements(preparedQuery, conn, EmptyBindingSet.getInstance(), null);
			} 
			 
			else {			
				WorkerUnionBase<BindingSet> union = FederationManager.getInstance().createWorkerUnion(queryInfo);
				
				for (StatementSource source : statementSources) {					
					Endpoint ownedEndpoint = EndpointManager.getEndpointManager().getEndpoint(source.getEndpointID());
					RepositoryConnection conn = ownedEndpoint.getConn();
					com.fluidops.fedx.evaluation.TripleSource t = ownedEndpoint.getTripleSource();
					union.addTask(new ParallelPreparedUnionTask(preparedQuery, t, conn, EmptyBindingSet.getInstance(), null));
				}
							
				result = union;
				// TODO we should add some DISTINCT here to have SET semantics
			}
		
			return result;
			
		} catch (Exception e) {
			throw new QueryEvaluationException(e);
		}
	}
	
	
	protected CloseableIteration<BindingSet, QueryEvaluationException> evaluateAtStatementSources(TupleExpr preparedQuery, List<StatementSource> statementSources, QueryInfo queryInfo) {
		
		try {
			CloseableIteration<BindingSet, QueryEvaluationException> result;
			
			if (statementSources.size()==1) {				
				Endpoint ownedEndpoint = EndpointManager.getEndpointManager().getEndpoint(statementSources.get(0).getEndpointID());
				RepositoryConnection conn = ownedEndpoint.getConn();
				com.fluidops.fedx.evaluation.TripleSource t = ownedEndpoint.getTripleSource();
				result = t.getStatements(preparedQuery, conn, EmptyBindingSet.getInstance(), null);
			} 
			 
			else {			
				WorkerUnionBase<BindingSet> union = FederationManager.getInstance().createWorkerUnion(queryInfo);
				
				for (StatementSource source : statementSources) {					
					Endpoint ownedEndpoint = EndpointManager.getEndpointManager().getEndpoint(source.getEndpointID());
					RepositoryConnection conn = ownedEndpoint.getConn();
					com.fluidops.fedx.evaluation.TripleSource t = ownedEndpoint.getTripleSource();
					union.addTask(new ParallelPreparedAlgebraUnionTask(preparedQuery, t, conn, EmptyBindingSet.getInstance(), null));					
				}
							
				result = union;
				
				// TODO we should add some DISTINCT here to have SET semantics
			}
		
			return result;
			
		} catch (Exception e) {
			throw new QueryEvaluationException(e);
		}
	}

}
