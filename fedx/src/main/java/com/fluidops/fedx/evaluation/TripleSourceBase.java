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

package com.fluidops.fedx.evaluation;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.Query;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;

import com.fluidops.fedx.algebra.ExclusiveGroup;
import com.fluidops.fedx.evaluation.iterator.GraphToBindingSetConversionIteration;
import com.fluidops.fedx.evaluation.iterator.SingleBindingSetIteration;
import com.fluidops.fedx.monitoring.Monitoring;
import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.structures.QueryType;
import com.fluidops.fedx.util.QueryStringUtil;

public abstract class TripleSourceBase implements TripleSource
{
	protected final Monitoring monitoringService;
	protected final Endpoint endpoint;

	public TripleSourceBase(Monitoring monitoring, Endpoint endpoint) {
		this.monitoringService = monitoring;
		this.endpoint = endpoint;
	}


	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> getStatements(
			String preparedQuery, RepositoryConnection conn, QueryType queryType)
			throws RepositoryException, MalformedQueryException,
			QueryEvaluationException
	{
		switch (queryType)
		{
		case SELECT:
			monitorRemoteRequest();
			TupleQuery tQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, preparedQuery);
			disableInference(tQuery);
			return tQuery.evaluate();
		case CONSTRUCT:
			monitorRemoteRequest();
			GraphQuery gQuery = conn.prepareGraphQuery(QueryLanguage.SPARQL, preparedQuery);
			disableInference(gQuery);
			return new GraphToBindingSetConversionIteration(gQuery.evaluate());
		case ASK:
			monitorRemoteRequest();
			BooleanQuery bQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, preparedQuery);
			disableInference(bQuery);
			return booleanToBindingSetIteration(bQuery.evaluate());
		default:
			throw new UnsupportedOperationException(
					"Operation not supported for query type " + queryType);
		}
	}
	

	@Override
	public boolean hasStatements(RepositoryConnection conn, Resource subj,
			IRI pred, Value obj, Resource... contexts) throws RepositoryException
	{
		return conn.hasStatement(subj, pred, obj, false, contexts);
	}
	
	
	@Override
	public boolean hasStatements(ExclusiveGroup group,
			RepositoryConnection conn, BindingSet bindings)
			throws RepositoryException, MalformedQueryException,
			QueryEvaluationException 	{
		
		monitorRemoteRequest();
		String preparedAskQuery = QueryStringUtil.askQueryString(group, bindings);
		return conn.prepareBooleanQuery(QueryLanguage.SPARQL, preparedAskQuery).evaluate();
	}


	protected void monitorRemoteRequest() {
		monitoringService.monitorRemoteRequest(endpoint);
	}
	
	private CloseableIteration<BindingSet, QueryEvaluationException> booleanToBindingSetIteration(boolean hasResult) {
		if (hasResult)
			return new SingleBindingSetIteration(EmptyBindingSet.getInstance());
		return new EmptyIteration<BindingSet, QueryEvaluationException>();
	}
	
	/**
	 * Set includeInference to disabled explicitly.
	 * 
	 * @param query
	 */
	protected void disableInference(Query query) {
		// set includeInferred to false explicitly
		try {
			query.setIncludeInferred(false);
		} catch (Exception e) { }
	}
	
}
