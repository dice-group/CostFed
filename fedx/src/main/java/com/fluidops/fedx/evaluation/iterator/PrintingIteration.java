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
import info.aduna.iteration.AbstractCloseableIteration;

import java.util.LinkedList;

import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;


/**
 * Print the bindings of the inner iteration to stdout, however maintain a copy, which
 * is accessible through this iteration.
 * 
 * @author Andreas Schwarte
 *
 */
public class PrintingIteration extends AbstractCloseableIteration<BindingSet, QueryEvaluationException> {

	protected final CloseableIteration<BindingSet, QueryEvaluationException> inner;
	protected LinkedList<BindingSet> copyQueue = new LinkedList<BindingSet>();
	protected boolean done = false;
	
	public PrintingIteration(
			CloseableIteration<BindingSet, QueryEvaluationException> inner) {
		super();
		this.inner = inner;
	}


	public void print() throws QueryEvaluationException {
		int count = 0;
		while (inner.hasNext()) {
			BindingSet item = inner.next();
			System.out.println(item);
			count++;
			synchronized (copyQueue) {
				copyQueue.addLast(item);
			}
		}
		done = true;
		System.out.println("Done with inner queue. Processed " + count + " items.");
	}
	
	
	
	
	@Override	
	public boolean hasNext() throws QueryEvaluationException {
		return !done || copyQueue.size()>0;
	}

	@Override
	public BindingSet next() throws QueryEvaluationException {
		synchronized (copyQueue) {
			return copyQueue.removeFirst();
		}
	}

	@Override
	public void remove() throws QueryEvaluationException {
		// TODO Auto-generated method stub
		
	}

	
	@Override
	protected void handleClose() throws QueryEvaluationException {
		inner.close();
		done=true;
		synchronized (copyQueue){
			copyQueue.clear();
		}
	}
}
