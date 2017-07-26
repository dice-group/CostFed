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

import java.util.concurrent.Callable;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

import com.fluidops.fedx.evaluation.FederationEvalStrategy;
import com.fluidops.fedx.structures.QueryInfo;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;



/**
 * Execute the nested loop join in a synchronous fashion, i.e. one binding after the other
 * 
 * @author Andreas Schwarte
 */
public class SynchronousJoin extends JoinExecutorBase<BindingSet> {

	public SynchronousJoin(FederationEvalStrategy strategy,
			CloseableIteration<BindingSet, QueryEvaluationException> leftIter,
			TupleExpr rightArg, BindingSet bindings, QueryInfo queryInfo)
	{
		super(strategy, leftIter, rightArg, bindings, queryInfo);
		handleBindings();
	}
	
	@Override
	protected void handleBindings() {
		
		int totalBindings=0;
		
		while (!closed && leftIter.hasNext()) {
			addTask(new Callable<CloseableIteration<BindingSet,QueryEvaluationException>>() {
				@Override
				public CloseableIteration<BindingSet, QueryEvaluationException> call() throws Exception {
					return strategy.evaluate(rightArg, leftIter.next());
				}
			});
			totalBindings++;
		}
			
		// XXX remove output if not needed anymore
		log.debug("JoinStats: left iter of join #" + this.joinId + " had " + totalBindings + " results.");
	}

	@Override
	protected void doAddTask(JoinExecutorBase<BindingSet>.JoinTask jt) {
		jt.run();
	}	
}
