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

import java.util.Iterator;
import java.util.List;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.ConvertingIteration;

import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

/**
 * Inserts original bindings into the result.
 * 
 * @author Andreas Schwarte
 */
public class BoundJoinConversionIteration extends ConvertingIteration<BindingSet, BindingSet, QueryEvaluationException>{

	protected final List<BindingSet> bindings;
	
	public BoundJoinConversionIteration(CloseableIteration<BindingSet, QueryEvaluationException> iter, List<BindingSet> bindings) {
		super(iter);
		this.bindings = bindings;
	}

	@Override
	protected BindingSet convert(BindingSet bIn) {
		QueryBindingSet res = new QueryBindingSet();
		int bIndex = -1;
		Iterator<Binding> bIter = bIn.iterator();
		while (bIter.hasNext()) {
			Binding b = bIter.next();
			String name = b.getName();
			bIndex = Integer.parseInt(name.substring(name.lastIndexOf("_")+1));
			res.addBinding(name.substring(0, name.lastIndexOf("_")), b.getValue());
		}
		res.addAll( bindings.get(bIndex));
		return res;
	}
}
