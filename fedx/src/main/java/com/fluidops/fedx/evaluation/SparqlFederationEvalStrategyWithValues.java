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

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;

import java.util.List;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.StatementPattern;

import com.fluidops.fedx.FedXConnection;
import com.fluidops.fedx.algebra.FilterTuple;
import com.fluidops.fedx.algebra.FilterValueExpr;
import com.fluidops.fedx.algebra.StatementTupleExpr;
import com.fluidops.fedx.evaluation.iterator.BoundJoinConversionIteration;
import com.fluidops.fedx.evaluation.iterator.BoundJoinVALUESConversionIteration;
import com.fluidops.fedx.evaluation.iterator.FilteringIteration;
import com.fluidops.fedx.util.QueryStringUtil;


/**
 * Implementation of a federation evaluation strategy which provides some
 * special optimizations for SPARQL (remote) endpoints. In addtion to the
 * optimizations from {@link SparqlFederationEvalStrategy} this implementation
 * uses the SPARQL 1.1 VALUES operator for the bound-join evaluation (with
 * a fallback to the pure SPARQL 1.0 UNION version).
 * 
 * @author Andreas Schwarte
 * @see BoundJoinConversionIteration
 * @since 3.0
 */
public class SparqlFederationEvalStrategyWithValues extends SparqlFederationEvalStrategy {

	
	public SparqlFederationEvalStrategyWithValues(FedXConnection conn) {
		super(conn);
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
		String preparedQuery = QueryStringUtil.selectQueryStringBoundJoinVALUES((StatementPattern)stmt, bindings, filterExpr, isEvaluated);
		
		CloseableIteration<BindingSet, QueryEvaluationException> result = evaluateAtStatementSources(preparedQuery, stmt.getStatementSources(), stmt.getQueryInfo());
						
		// apply filter and/or convert to original bindings
		if (filterExpr!=null && !isEvaluated) {
			result = new BoundJoinVALUESConversionIteration(result, bindings);		// apply conversion
			result = new FilteringIteration(this, filterExpr, result);				// apply filter
			if (!result.hasNext())	
				return new EmptyIteration<BindingSet, QueryEvaluationException>();
		} else {
			result = new BoundJoinVALUESConversionIteration(result, bindings);
		}
			
		return result;		
	}

}
