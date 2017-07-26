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
import org.eclipse.rdf4j.common.iteration.AbstractCloseableIteration;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;

import com.fluidops.fedx.QueryManager;
import com.fluidops.fedx.structures.QueryInfo;


/**
 * An iteration which wraps the final result and in case of exceptions aborts query evaluation
 * for the corresponding query in fedx (potentially subqueries are still running, and jobs are
 * scheduled). 
 * 
 * If some external component calls close() on this iteration AND if the corresponding query
 * is still running, the query is aborted within FedX. An example case would be Sesame's 
 * QueryInteruptIterations, which is used to enforce maxQueryTime.
 * 
 * If the query is finished, the FederationManager is notified that the query is done, and the
 * query is removed from the set of running queries.
 * 
 * @author Andreas Schwarte
 *
 */
public class QueryResultIteration extends AbstractCloseableIteration<BindingSet, QueryEvaluationException>  {

	// TODO apply this class and provide test case
	
	protected final CloseableIteration<BindingSet, QueryEvaluationException> inner;
	protected final QueryInfo queryInfo;
	
	public QueryResultIteration(
			CloseableIteration<BindingSet, QueryEvaluationException> inner, QueryInfo queryInfo) {
		super();
		this.inner = inner;
		this.queryInfo = queryInfo;
	}
	
	
	@Override	
	public boolean hasNext() throws QueryEvaluationException {
		if (inner.hasNext())
			return true;
		else {
			// inform the query manager that this query is done
		    queryInfo.getQueryManager().finishQuery(queryInfo);
			return false;
		}
	}

	@Override
	public BindingSet next() throws QueryEvaluationException {
		try {
			BindingSet next = inner.next();
			if (next==null)
			    queryInfo.getQueryManager().finishQuery(queryInfo);
			return next;
		} catch (QueryEvaluationException e){
			abortQuery();
			throw e;
		}
	}

	@Override
	public void remove() {
		inner.remove();		
	}

	
	@Override
	protected void handleClose() throws QueryEvaluationException {
		inner.close();
		abortQuery();
	}
	

	/**
	 * Abort the query in the schedulers if it is still running.
	 */
	protected void abortQuery() {
		QueryManager qm = queryInfo.getQueryManager();
		if (qm.isRunning(queryInfo))
			qm.abortQuery(queryInfo);
	}
}
