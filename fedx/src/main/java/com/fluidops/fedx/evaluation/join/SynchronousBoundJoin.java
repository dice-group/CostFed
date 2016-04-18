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

package com.fluidops.fedx.evaluation.join;

import info.aduna.iteration.CloseableIteration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;

import com.fluidops.fedx.Config;
import com.fluidops.fedx.algebra.CheckStatementPattern;
import com.fluidops.fedx.algebra.IndependentJoinGroup;
import com.fluidops.fedx.algebra.StatementTupleExpr;
import com.fluidops.fedx.evaluation.FederationEvalStrategy;
import com.fluidops.fedx.exception.FedXRuntimeException;
import com.fluidops.fedx.structures.QueryInfo;



/**
 * Execute the nested loop join in a synchronous fashion, using grouped requests,
 * i.e. group bindings into one SPARQL request using the UNION operator
 * 
 * @author Andreas Schwarte
 */
public class SynchronousBoundJoin extends SynchronousJoin {

	public static Logger log = Logger.getLogger(SynchronousBoundJoin.class);
	
	
	public SynchronousBoundJoin(FederationEvalStrategy strategy,
			CloseableIteration<BindingSet, QueryEvaluationException> leftIter,
			TupleExpr rightArg, BindingSet bindings, QueryInfo queryInfo)
			throws QueryEvaluationException {
		super(strategy, leftIter, rightArg, bindings, queryInfo);
	}

	
	
	@Override
	protected void handleBindings() {
		
		// XXX use something else as second check, e.g. an empty interface
		if (! ((rightArg instanceof StatementPattern) || (rightArg instanceof IndependentJoinGroup) )) {
			log.warn("Right argument is not a StatementPattern nor a IndependentJoinGroup. Fallback on SynchronousJoin implementation: " + rightArg.getClass().getCanonicalName());
			super.handleBindings();	// fallback
			return;
		}
		
		int nBindingsCfg = Config.getConfig().getBoundJoinBlockSize();	
		int totalBindings = 0;		// the total number of bindings
		StatementTupleExpr stmt = (StatementTupleExpr)rightArg;
		
		
		
		// optimization: if there is no free variable, we can avoid the bound-join
		// first item is always sent in a non-bound way
		
		// TODO independent join group!!!! (see ControlledWorkerBoundJoin)
		if (rightArg instanceof IndependentJoinGroup)
			throw new FedXRuntimeException("Synchronous Bound joins does not support Independent join group yet");
		
		boolean hasFreeVars = true;
		if (!closed && leftIter.hasNext()) {
			BindingSet b = leftIter.next();
			totalBindings++;
			hasFreeVars = stmt.hasFreeVarsFor(b);
			if (!hasFreeVars)
				stmt = new CheckStatementPattern(stmt);
			final CloseableIteration<BindingSet, QueryEvaluationException> cit = strategy.evaluate(stmt, b);
			addTask(new Callable<CloseableIteration<BindingSet,QueryEvaluationException>>() 
			{
				@Override
				public CloseableIteration<BindingSet, QueryEvaluationException> call() throws Exception {
					return cit;
				}
			});
		}
		
		int nBindings;
		List<BindingSet> bindings = null;
		while (!closed && leftIter.hasNext()) {
			
			/*
			 * XXX idea:
			 * 
			 * make nBindings dependent on the number of intermediate results of the left argument.
			 * 
			 * If many intermediate results, increase the number of bindings. This will result in less
			 * remote SPARQL requests.
			 * 
			 */
			if (totalBindings>10)
				nBindings = nBindingsCfg;
			else
				nBindings = 3;

			bindings = new ArrayList<BindingSet>(nBindings);
			
			int count=0;
			while (count < nBindings && leftIter.hasNext()) {
				bindings.add(leftIter.next());
				count++;
			}
			
			totalBindings += count;		
			
			final CloseableIteration<BindingSet, QueryEvaluationException> cit =
					hasFreeVars ? strategy.evaluateBoundJoinStatementPattern(stmt, bindings) :
						strategy.evaluateGroupedCheck((CheckStatementPattern)stmt, bindings);
					
			addTask(new Callable<CloseableIteration<BindingSet,QueryEvaluationException>>() {
				@Override
				public CloseableIteration<BindingSet, QueryEvaluationException> call() throws Exception {
					return cit;
				}
			});
		}
		
		log.debug("JoinStats: left iter of join #" + this.joinId + " had " + totalBindings + " results.");
						
	}
}
