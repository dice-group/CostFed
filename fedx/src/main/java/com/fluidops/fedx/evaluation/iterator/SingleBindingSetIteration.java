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

import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;

/**
 * Represents an iteration that contains only a single binding set.
 * 
 * @author Andreas Schwarte
 *
 */
public class SingleBindingSetIteration extends AbstractCloseableIteration<BindingSet, QueryEvaluationException>{

	protected final BindingSet res;
	protected boolean hasNext = true;
	
	
	public SingleBindingSetIteration(BindingSet res) {
		super();
		this.res = res;
	}

	
	@Override
	public boolean hasNext() {
		return hasNext;
	}

	@Override
	public BindingSet next() {
		hasNext = false;
		return res;
	}

	@Override
	public void remove() {
		// no-op		
	}
	
	@Override
	protected void handleClose() throws QueryEvaluationException {
		hasNext=false;
	}
}
