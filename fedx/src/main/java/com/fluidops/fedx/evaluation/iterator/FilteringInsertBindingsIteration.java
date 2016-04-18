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

import info.aduna.iteration.CloseableIteration;

import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import com.fluidops.fedx.algebra.FilterValueExpr;


/**
 * Filters iteration according to specified filterExpr and inserts original 
 * bindings into filtered results.
 * 
 * @author Andreas Schwarte
 */
public class FilteringInsertBindingsIteration extends FilteringIteration {

	protected final BindingSet bindings;
	
	public FilteringInsertBindingsIteration(FilterValueExpr filterExpr, BindingSet bindings,
			CloseableIteration<BindingSet, QueryEvaluationException> iter)
			throws QueryEvaluationException {
		super(filterExpr, iter);
		this.bindings = bindings;
	}
	
	@Override
	public BindingSet next() throws QueryEvaluationException {
		BindingSet next = super.next();
		if (next==null)
			return null;
		QueryBindingSet res = new QueryBindingSet(bindings.size() + next.size());
		res.addAll(bindings);
		res.addAll(next);
		return res;
	}
}
