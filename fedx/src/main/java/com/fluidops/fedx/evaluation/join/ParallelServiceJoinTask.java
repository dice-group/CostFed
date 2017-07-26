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

import java.util.List;
import java.util.concurrent.Callable;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;

import com.fluidops.fedx.algebra.FedXService;
import com.fluidops.fedx.evaluation.FederationEvalStrategy;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;

/**
 * A task implementation representing the evaluation of a SERVICE which
 * is to be evaluated using block input. See {@link FederationEvalStrategy#evaluateService(FedXService, List)
 * for details.
 * 
 * @author Andreas Schwarte
 */
public class ParallelServiceJoinTask implements Callable<CloseableIteration<BindingSet, QueryEvaluationException>> {
	
	protected final FederationEvalStrategy strategy;
	protected final FedXService expr;
	protected final List<BindingSet> bindings;
	
	public ParallelServiceJoinTask(FederationEvalStrategy strategy, FedXService expr, List<BindingSet> bindings) {
		this.strategy = strategy;
		this.expr = expr;
		this.bindings = bindings;
	}

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> call() {
		return strategy.evaluateService(expr, bindings);
	}
}
