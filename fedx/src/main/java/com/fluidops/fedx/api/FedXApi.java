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

package com.fluidops.fedx.api;

import java.util.List;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryResult;

import com.fluidops.fedx.exception.FedXException;
import com.fluidops.fedx.structures.Endpoint;

public interface FedXApi {

	public TupleQueryResult evaluate(String query) throws QueryEvaluationException;
	
	public TupleQueryResult evaluate(String query, List<Endpoint> endpoints) throws FedXException, QueryEvaluationException;
	
	public TupleQueryResult evaluateAt(String query, List<String> endpointIds) throws FedXException, QueryEvaluationException;
	
	public RepositoryResult<Statement> getStatements(Resource subject, IRI predicate, Value object, Resource... contexts );

	public void addEndpoint(Endpoint e);

	public void removeEndpoint(Endpoint e);
	
	public void removeEndpoint(String endpointId);
	
	public void shutdown();
}
