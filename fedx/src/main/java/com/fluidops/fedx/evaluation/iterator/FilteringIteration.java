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

package com.fluidops.fedx.evaluation.iterator;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.FilterIteration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;

import com.fluidops.fedx.algebra.FilterValueExpr;
import com.fluidops.fedx.evaluation.FederationEvalStrategy;

/**
 * Filters iteration according to specified filterExpr.
 * 
 * @author Andreas Schwarte
 */
public class FilteringIteration extends FilterIteration<BindingSet, QueryEvaluationException> {
	
	public static Logger log = LoggerFactory.getLogger(FilteringIteration.class);	
	
	protected FilterValueExpr filterExpr;
	protected FederationEvalStrategy strategy;
	
	public FilteringIteration(FederationEvalStrategy strategy, FilterValueExpr filterExpr, CloseableIteration<BindingSet, QueryEvaluationException> iter) throws QueryEvaluationException {
		super(iter);
		this.filterExpr = filterExpr;
		this.strategy = strategy;
	}	
	
	@Override
	protected boolean accept(BindingSet bindings) throws QueryEvaluationException {
		try {
			return strategy.isTrue(filterExpr, bindings);
		} catch (ValueExprEvaluationException e) {
			log.warn("Failed to evaluate filter expr: " + e.getMessage());
			// failed to evaluate condition
			return false;
		}
	}
}
