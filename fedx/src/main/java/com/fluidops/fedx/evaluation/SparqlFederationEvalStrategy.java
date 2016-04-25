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

import org.apache.commons.lang3.NotImplementedException;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.repository.RepositoryConnection;

import com.fluidops.fedx.algebra.CheckStatementPattern;
import com.fluidops.fedx.algebra.ExclusiveGroup;
import com.fluidops.fedx.algebra.FilterTuple;
import com.fluidops.fedx.algebra.FilterValueExpr;
import com.fluidops.fedx.algebra.IndependentJoinGroup;
import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.algebra.StatementTupleExpr;
import com.fluidops.fedx.evaluation.concurrent.ControlledWorkerScheduler;
import com.fluidops.fedx.evaluation.iterator.BoundJoinConversionIteration;
import com.fluidops.fedx.evaluation.iterator.BufferedCloseableIterator;
import com.fluidops.fedx.evaluation.iterator.FilteringIteration;
import com.fluidops.fedx.evaluation.iterator.GroupedCheckConversionIteration;
import com.fluidops.fedx.evaluation.iterator.IndependentJoingroupBindingsIteration;
import com.fluidops.fedx.evaluation.iterator.IndependentJoingroupBindingsIteration3;
import com.fluidops.fedx.evaluation.iterator.QueueIteration;
import com.fluidops.fedx.evaluation.iterator.SingleBindingSetIteration;
import com.fluidops.fedx.evaluation.join.ControlledWorkerBoundJoin;
import com.fluidops.fedx.exception.IllegalQueryException;
import com.fluidops.fedx.structures.QueryInfo;
import com.fluidops.fedx.util.QueryStringUtil;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.EmptyIteration;


/**
 * Implementation of a federation evaluation strategy which provides some
 * special optimizations for SPARQL (remote) endpoints. The most
 * important optimization is to used prepared SPARQL Queries that are already 
 * created using Strings. 
 * 
 * Joins are executed using {@link ControlledWorkerBoundJoin}.
 * 
 * @author Andreas Schwarte
 *
 */
public class SparqlFederationEvalStrategy extends FederationEvalStrategy {

	
	public SparqlFederationEvalStrategy() {
		
	}
	
	
	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluateBoundJoinStatementPattern(
			StatementTupleExpr stmt, List<BindingSet> bindings)
	{
		// we can omit the bound join handling
		if (bindings.size()==1)
			return evaluate(stmt, bindings.get(0));
				
		FilterValueExpr filterExpr = null;
		if (stmt instanceof FilterTuple)
			filterExpr = ((FilterTuple)stmt).getFilterExpr();
		
		Boolean isEvaluated = false;
		String preparedQuery = QueryStringUtil.selectQueryStringBoundUnion((StatementPattern)stmt, bindings, filterExpr, isEvaluated);
		
		CloseableIteration<BindingSet, QueryEvaluationException> result = evaluateAtStatementSources(preparedQuery, stmt.getStatementSources(), stmt.getQueryInfo());
						
		// apply filter and/or convert to original bindings
		if (filterExpr!=null && !isEvaluated) {
			result = new BoundJoinConversionIteration(result, bindings);		// apply conversion
			result = new FilteringIteration(filterExpr, result);				// apply filter
			if (!result.hasNext()) {
				result.close();
				return new EmptyIteration<BindingSet, QueryEvaluationException>();
			}
		} else {
			result = new BoundJoinConversionIteration(result, bindings);
		}
			
		// in order to avoid leakage of http route during the iteration
		return new BufferedCloseableIterator<BindingSet, QueryEvaluationException>(result);		
	}

	
	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluateGroupedCheck(
			CheckStatementPattern stmt, List<BindingSet> bindings)
	{

		if (bindings.size()==1)
			return stmt.evaluate(bindings.get(0));
		
		String preparedQuery = QueryStringUtil.selectQueryStringBoundCheck(stmt.getStatementPattern(), bindings);
					
		CloseableIteration<BindingSet, QueryEvaluationException> result = evaluateAtStatementSources(preparedQuery, stmt.getStatementSources(), stmt.getQueryInfo());
		
		// in order to avoid licking http route while iteration
		return new BufferedCloseableIterator<BindingSet, QueryEvaluationException>(
			new GroupedCheckConversionIteration(result, bindings)
		); 	
	}


	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluateIndependentJoinGroup(
			IndependentJoinGroup joinGroup, BindingSet bindings)
	{
			
		String preparedQuery = QueryStringUtil.selectQueryStringIndependentJoinGroup(joinGroup, bindings);
		
		try {
			List<StatementSource> statementSources = joinGroup.getMembers().get(0).getStatementSources();	// TODO this is only correct for the prototype (=> different endpoints)
			CloseableIteration<BindingSet, QueryEvaluationException> result = evaluateAtStatementSources(preparedQuery, statementSources, joinGroup.getQueryInfo());
						
			// return only those elements which evaluated positively at the endpoint
			result = new IndependentJoingroupBindingsIteration(result, bindings);
			
			// in order to avoid licking http route while iteration
			return new BufferedCloseableIterator<BindingSet, QueryEvaluationException>(result);
		} catch (Exception e) {
			throw new QueryEvaluationException(e);
		}

	}


	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluateIndependentJoinGroup(
			IndependentJoinGroup joinGroup, List<BindingSet> bindings)
	{
				
		String preparedQuery = QueryStringUtil.selectQueryStringIndependentJoinGroup(joinGroup, bindings);
		
		try {
			List<StatementSource> statementSources = joinGroup.getMembers().get(0).getStatementSources();	// TODO this is only correct for the prototype (=> different endpoints)
			CloseableIteration<BindingSet, QueryEvaluationException> result = evaluateAtStatementSources(preparedQuery, statementSources, joinGroup.getQueryInfo());
						
			// return only those elements which evaluated positively at the endpoint
//			result = new IndependentJoingroupBindingsIteration2(result, bindings);
			result = new IndependentJoingroupBindingsIteration3(result, bindings);
			
			// in order to avoid licking http route while iteration
			return new BufferedCloseableIterator<BindingSet, QueryEvaluationException>(result);
		} catch (Exception e) {
			throw new QueryEvaluationException(e);
		}
	}


	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> executeJoin(
			ControlledWorkerScheduler joinScheduler,
			CloseableIteration<BindingSet, QueryEvaluationException> leftIter,
			TupleExpr rightArg, BindingSet bindings, QueryInfo queryInfo)
	{
		ControlledWorkerBoundJoin join = new ControlledWorkerBoundJoin(joinScheduler, this, leftIter, rightArg, bindings, queryInfo);
		return join;
	}


	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluateExclusiveGroup(
			ExclusiveGroup group, RepositoryConnection conn,
			TripleSource tripleSource, BindingSet bindings)
	{
		
		Boolean isEvaluated = false;
			
		try  {
			String preparedQuery = QueryStringUtil.selectQueryString(group, bindings, group.getFilterExpr(), isEvaluated);
			// in order to avoid licking http route while iteration
			return new BufferedCloseableIterator<BindingSet, QueryEvaluationException>(
				tripleSource.getStatements(preparedQuery, conn, bindings, (isEvaluated ? null : group.getFilterExpr()))
			);
		} catch (IllegalQueryException e) {
			/* no projection vars, e.g. local vars only, can occur in joins */
			if (tripleSource.hasStatements(group, conn, bindings))
				return new SingleBindingSetIteration(bindings);
			return new EmptyIteration<BindingSet, QueryEvaluationException>();
		}		
		
	}


	@Override
	public void evaluate(QueueIteration<BindingSet> qit, TupleExpr expr, List<BindingSet> bindings) {
		throw new NotImplementedException("evaluate");
	}
}
