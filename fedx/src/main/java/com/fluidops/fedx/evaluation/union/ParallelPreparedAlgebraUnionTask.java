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

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.repository.RepositoryConnection;

import com.fluidops.fedx.EndpointManager;
import com.fluidops.fedx.algebra.FilterValueExpr;
import com.fluidops.fedx.evaluation.TripleSource;
import com.fluidops.fedx.structures.Endpoint;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;

/**
 * A task implementation representing a prepared union, i.e. the prepared query is executed
 * on the provided triple source.
 * 
 * @author Andreas Schwarte
 */
public class ParallelPreparedAlgebraUnionTask implements Callable<CloseableIteration<BindingSet, QueryEvaluationException>> {
	
	protected final TripleSource tripleSource;
	protected final Endpoint ep;
	protected final TupleExpr preparedQuery;
	protected final BindingSet bindings;
	protected final FilterValueExpr filterExpr;
	
	public ParallelPreparedAlgebraUnionTask(TupleExpr preparedQuery, TripleSource tripleSource, Endpoint ep, BindingSet bindings, FilterValueExpr filterExpr) {
		this.preparedQuery = preparedQuery;
		this.bindings = bindings;
		this.tripleSource = tripleSource;
		this.ep = ep;
		this.filterExpr = filterExpr;
	}

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> call() {
		return tripleSource.getStatements(preparedQuery, ep.getConn(), bindings, filterExpr);
	}

	public String toString() {
		return this.getClass().getSimpleName() + " @" + ep.getId() + ": " + preparedQuery.toString();
	}
}
