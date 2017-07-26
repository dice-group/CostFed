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
import org.eclipse.rdf4j.common.iteration.ConvertingIteration;

import java.util.Iterator;
import java.util.List;

import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import com.fluidops.fedx.evaluation.SparqlFederationEvalStrategyWithValues;
import com.fluidops.fedx.util.QueryStringUtil;

/**
 * Inserts original bindings into the result. This implementation is used for
 * bound joins with VALUES clauses, see {@link SparqlFederationEvalStrategyWithValues}.
 * 
 * It is assumed the the query results contain a binding for "?__index" which corresponds
 * to the index in the input mappings. See {@link QueryStringUtil} for details
 * 
 * @author Andreas Schwarte
 * @see SparqlFederationEvalStrategyWithValues
 * @since 3.0
 */
public class BoundJoinVALUESConversionIteration extends ConvertingIteration<BindingSet, BindingSet, QueryEvaluationException>{

	/**
	 * The binding name for the index
	 */
	public static final String INDEX_BINDING_NAME = "__index";
	
	protected final List<BindingSet> bindings;
	
	public BoundJoinVALUESConversionIteration(CloseableIteration<BindingSet, QueryEvaluationException> iter, List<BindingSet> bindings) {
		super(iter);
		this.bindings = bindings;
	}

	@Override
	protected BindingSet convert(BindingSet bIn) throws QueryEvaluationException {
		QueryBindingSet res = new QueryBindingSet();
		int bIndex = Integer.parseInt(bIn.getBinding(INDEX_BINDING_NAME).getValue().stringValue());
		Iterator<Binding> bIter = bIn.iterator();
		while (bIter.hasNext()) {
			Binding b = bIter.next();
			if (b.getName().equals(INDEX_BINDING_NAME))
				continue;
			res.addBinding(b);
		}
		for (Binding bs : bindings.get(bIndex))
			res.setBinding(bs);
		return res;
	}
}
