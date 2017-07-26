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

package com.fluidops.fedx.evaluation.union;

import java.util.concurrent.Callable;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.StatementPattern;

import com.fluidops.fedx.algebra.FilterValueExpr;
import com.fluidops.fedx.evaluation.TripleSource;
import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.util.QueryStringUtil;

/**
 * A task implementation representing a statement expression to be evaluated.
 * 
 * @author Andreas Schwarte
 */
public class ParallelUnionTask implements Callable<CloseableIteration<BindingSet, QueryEvaluationException>> {
	
	protected final TripleSource tripleSource;
	protected final Endpoint ep;
	protected final StatementPattern stmt;
	protected final BindingSet bindings;
	protected final FilterValueExpr filterExpr;
	
	public ParallelUnionTask(StatementPattern stmt, TripleSource tripleSource, Endpoint ep, BindingSet bindings, FilterValueExpr filterExpr) {
		this.stmt = stmt;
		this.bindings = bindings;
		this.tripleSource = tripleSource;
		this.ep = ep;
		this.filterExpr = filterExpr;
	}

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> call() {
		return tripleSource.getStatements(stmt, ep.getConn(), bindings, filterExpr);
	}
	
	public String toString() {
		return this.getClass().getSimpleName() + " @" + ep.getId() + ": " + QueryStringUtil.toString(stmt);
	}
}
