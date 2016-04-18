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

import java.util.Iterator;
import java.util.Set;

import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.Service;
import org.openrdf.query.algebra.evaluation.federation.FederatedService;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import com.fluidops.fedx.evaluation.iterator.InsertBindingsIteration;
import com.fluidops.fedx.structures.Endpoint;

import info.aduna.iteration.CloseableIteration;

/**
 * A {@link FederatedService} which is registered for native store
 * sails
 * 
 */
@Deprecated
public class SAILFederatedService implements FederatedService {

	private Endpoint endpoint;
	
	public SAILFederatedService(Endpoint endpoint) {
		this.endpoint = endpoint;
	}

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
			Service service,
			CloseableIteration<BindingSet, QueryEvaluationException> bindings,
			String baseUri) throws QueryEvaluationException {
		
		throw new UnsupportedOperationException("NOT YET IMPLEMENTED");
	}

	@Override
	public boolean ask(Service service, BindingSet bindings, String baseUri) throws QueryEvaluationException {
		RepositoryConnection conn = endpoint.getConn();
		try {
			BooleanQuery query = conn.prepareBooleanQuery(QueryLanguage.SPARQL, service.getAskQueryString(), baseUri);
			Iterator<Binding> bIter = bindings.iterator();
			while (bIter.hasNext()) {
				Binding b = bIter.next();
				if (service.getServiceVars().contains(b.getName()))
					query.setBinding(b.getName(), b.getValue());
			}
			return query.evaluate();
		} catch(Throwable e) {
			throw new QueryEvaluationException(e);
		} finally {
			conn.close();
		}
	}

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> select(Service service, Set<String> projectionVars, BindingSet bindings, String baseUri) throws QueryEvaluationException {
		RepositoryConnection conn = endpoint.getConn();
		try {
			TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, service.getSelectQueryString(projectionVars), baseUri);
			Iterator<Binding> bIter = bindings.iterator();
			while (bIter.hasNext()) {
				Binding b = bIter.next();
				if (service.getServiceVars().contains(b.getName()))
					query.setBinding(b.getName(), b.getValue());
			}
			TupleQueryResult qRes = query.evaluate();
			return new InsertBindingsIteration(qRes, bindings);
		} catch(Throwable e) {
			throw new QueryEvaluationException(e);
		} finally {
			conn.close();
		}
	}

	@Override
	public void initialize() throws RepositoryException {
	}

	@Override
	public void shutdown() throws RepositoryException {
	}

	@Override
	public boolean isInitialized() {
		return true;
	}
}
