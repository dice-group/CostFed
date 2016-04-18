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

import info.aduna.iteration.AbstractCloseableIteration;

import java.util.NoSuchElementException;

import org.openrdf.model.Statement;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;


/**
 * Converts graph results into a binding set iteration
 * 
 * @author Andreas Schwarte
 */
public class GraphToBindingSetConversionIteration extends AbstractCloseableIteration<BindingSet, QueryEvaluationException>{

	protected final GraphQueryResult graph;

	
	public GraphToBindingSetConversionIteration(GraphQueryResult graph) {
		super();
		this.graph = graph;
	}
	
	
	@Override
	public boolean hasNext() throws QueryEvaluationException {
		return graph.hasNext();
	}

	@Override
	public BindingSet next() throws QueryEvaluationException {

		try {
			return convert(graph.next());
		} catch(NoSuchElementException e) {
			throw e;
	    } catch(IllegalStateException e) {
	    	throw e;
	    } 
	}

	@Override
	public void remove() throws QueryEvaluationException {

		try {
			graph.remove();
		} catch(UnsupportedOperationException e) {
			throw e;
		} catch(IllegalStateException e) {
			throw e;
		} 		
	}

	
	protected BindingSet convert(Statement st) {
		QueryBindingSet result = new QueryBindingSet();
		result.addBinding("subject", st.getSubject());
		result.addBinding("predicate", st.getPredicate());
		result.addBinding("object", st.getObject());
		if (st.getContext() != null) {
			result.addBinding("context", st.getContext());
		}

		return result;
	}
	
	
}
