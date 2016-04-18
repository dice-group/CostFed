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

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.EmptyIteration;

import java.util.List;

import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import com.fluidops.fedx.algebra.CheckStatementPattern;
import com.fluidops.fedx.algebra.ExclusiveGroup;
import com.fluidops.fedx.algebra.FilterTuple;
import com.fluidops.fedx.algebra.FilterValueExpr;
import com.fluidops.fedx.algebra.IndependentJoinGroup;
import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.algebra.StatementTupleExpr;
import com.fluidops.fedx.evaluation.concurrent.ControlledWorkerScheduler;
import com.fluidops.fedx.evaluation.iterator.BoundJoinConversionIteration;
import com.fluidops.fedx.evaluation.iterator.FilteringIteration;
import com.fluidops.fedx.evaluation.iterator.GroupedCheckConversionIteration;
import com.fluidops.fedx.evaluation.iterator.IndependentJoingroupBindingsIteration;
import com.fluidops.fedx.evaluation.iterator.IndependentJoingroupBindingsIteration3;
import com.fluidops.fedx.evaluation.join.ControlledWorkerJoin;
import com.fluidops.fedx.structures.QueryInfo;
import com.fluidops.fedx.util.QueryAlgebraUtil;
import com.fluidops.fedx.util.QueryStringUtil;


/**
 * Implementation of a federation evaluation strategy which provides some
 * special optimizations for Native (local) Sesame repositories. The most
 * important optimization is to use prepared Queries that are already 
 * created in the internal representation used by Sesame. This is necessary
 * to avoid String parsing overhead.  
 * 
 * Joins are executed using {@link ControlledWorkerJoin}
 * @author Andreas Schwarte
 *
 */
public class SailFederationEvalStrategy extends FederationEvalStrategy {

	
	public SailFederationEvalStrategy() {
		
	}
	
	
	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluateBoundJoinStatementPattern(
			StatementTupleExpr stmt, List<BindingSet> bindings)
			throws QueryEvaluationException {
				
		// we can omit the bound join handling
		if (bindings.size()==1)
			return evaluate(stmt, bindings.get(0));
				
		FilterValueExpr filterExpr = null;
		if (stmt instanceof FilterTuple)
			filterExpr = ((FilterTuple)stmt).getFilterExpr();
		
		Boolean isEvaluated = false;
		TupleExpr preparedQuery = QueryAlgebraUtil.selectQueryBoundUnion((StatementPattern)stmt, bindings, filterExpr, isEvaluated);
		
		CloseableIteration<BindingSet, QueryEvaluationException> result = evaluateAtStatementSources(preparedQuery, stmt.getStatementSources(), stmt.getQueryInfo());
						
		// apply filter and/or convert to original bindings
		if (filterExpr!=null && !isEvaluated) {
			result = new BoundJoinConversionIteration(result, bindings);		// apply conversion
			result = new FilteringIteration(filterExpr, result);				// apply filter
			if (!result.hasNext())	
				return new EmptyIteration<BindingSet, QueryEvaluationException>();
		} else {
			result = new BoundJoinConversionIteration(result, bindings);
		}
			
		return result;		
	}

	
	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluateGroupedCheck(
			CheckStatementPattern stmt, List<BindingSet> bindings)
			throws QueryEvaluationException  {

		if (bindings.size()==1)
			return stmt.evaluate(bindings.get(0));
		
		TupleExpr preparedQuery = QueryAlgebraUtil.selectQueryStringBoundCheck(stmt.getStatementPattern(), bindings);
				
		CloseableIteration<BindingSet, QueryEvaluationException> result = evaluateAtStatementSources(preparedQuery, stmt.getStatementSources(), stmt.getQueryInfo());
		
		return new GroupedCheckConversionIteration(result, bindings); 	
	}


	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluateIndependentJoinGroup(
			IndependentJoinGroup joinGroup, BindingSet bindings)
			throws QueryEvaluationException {
			
		TupleExpr preparedQuery = QueryAlgebraUtil.selectQueryIndependentJoinGroup(joinGroup, bindings);
		
		try {
			List<StatementSource> statementSources = joinGroup.getMembers().get(0).getStatementSources();	// TODO this is only correct for the prototype (=> different endpoints)
			CloseableIteration<BindingSet, QueryEvaluationException> result = evaluateAtStatementSources(preparedQuery, statementSources, joinGroup.getQueryInfo());
						
			// return only those elements which evaluated positively at the endpoint
			result = new IndependentJoingroupBindingsIteration(result, bindings);
			
			return result;
		} catch (Exception e) {
			throw new QueryEvaluationException(e);
		}

	}


	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluateIndependentJoinGroup(
			IndependentJoinGroup joinGroup, List<BindingSet> bindings)
			throws QueryEvaluationException  {
				
		String preparedQuery = QueryStringUtil.selectQueryStringIndependentJoinGroup(joinGroup, bindings);
		
		try {
			List<StatementSource> statementSources = joinGroup.getMembers().get(0).getStatementSources();	// TODO this is only correct for the prototype (=> different endpoints)
			CloseableIteration<BindingSet, QueryEvaluationException> result = evaluateAtStatementSources(preparedQuery, statementSources, joinGroup.getQueryInfo());
						
			// return only those elements which evaluated positively at the endpoint
//			result = new IndependentJoingroupBindingsIteration2(result, bindings);
			result = new IndependentJoingroupBindingsIteration3(result, bindings);
			
			return result;
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
		
		ControlledWorkerJoin join = new ControlledWorkerJoin(joinScheduler, this, leftIter, rightArg, bindings, queryInfo);
		return join;
	}


	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluateExclusiveGroup(
			ExclusiveGroup group, RepositoryConnection conn,
			TripleSource tripleSource, BindingSet bindings)
			throws RepositoryException, MalformedQueryException,
			QueryEvaluationException {

		// simple thing: use a prepared query
		Boolean isEvaluated = false;
		TupleExpr preparedQuery = QueryAlgebraUtil.selectQuery(group, bindings, group.getFilterExpr(), isEvaluated);
		return tripleSource.getStatements(preparedQuery, conn, bindings, (isEvaluated ? null : group.getFilterExpr()));
	
		// other option (which might be faster for sesame native stores): join over the statements
		// TODO implement this and evaluate if it is faster ..

	}	
		
	
}
