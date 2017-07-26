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

package com.fluidops.fedx.sail;

import java.util.Properties;

import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.repository.sail.SailBooleanQuery;
import org.eclipse.rdf4j.repository.sail.SailGraphQuery;
import org.eclipse.rdf4j.repository.sail.SailQuery;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailTupleQuery;

import com.fluidops.fedx.FedX;
import com.fluidops.fedx.FedXConnection;
import com.fluidops.fedx.QueryManager;
import com.fluidops.fedx.structures.QueryType;

/**
 * A special {@link SailRepositoryConnection} which adds the original query
 * string as binding to the returned query. The binding name is defined by
 * {@link #BINDING_ORIGINAL_QUERY} and is added to all query instances returned
 * by the available prepare methods.
 * 
 * @author Andreas Schwarte
 *
 */
public class FedXSailRepositoryConnection extends SailRepositoryConnection
{
    protected final QueryManager queryManager;
    
	/**
	 * We add a binding to each parsed query mapping the original query
	 * in order to send the original query to the endpoint if there is
	 * only a single federation member is relevant for this query.
	 */
	public static final String BINDING_ORIGINAL_QUERY = "__originalQuery";
	public static final String BINDING_ORIGINAL_QUERY_TYPE = "__originalQueryType";
	
	FedXSailRepositoryConnection(SailRepository repository, FedXConnection sailConnection)
	{
		super(repository, sailConnection);
		queryManager = new QueryManager(this);
		sailConnection.setQqueryManager(queryManager);
		
        Properties props = ((FedX)repository.getSail()).getPrefixDeclarations();
        if (props != null) {
            for (String ns : props.stringPropertyNames()) {
                queryManager.addPrefixDeclaration(ns, props.getProperty(ns)); // register namespace/prefix pair
            }
        }
	}

	@Override
	public FedXConnection getSailConnection() {
	    return (FedXConnection)super.getSailConnection();
	}
	
	@Override
	public SailQuery prepareQuery(QueryLanguage ql, String queryString,
			String baseURI)
	{
		SailQuery q = super.prepareQuery(ql, queryString, baseURI);
		if (q instanceof TupleQuery)
			insertOriginalQueryString(q, queryString, QueryType.SELECT);
		else if (q instanceof GraphQuery)
			insertOriginalQueryString(q, queryString, QueryType.CONSTRUCT);
		else if (q instanceof BooleanQuery)
			insertOriginalQueryString(q, queryString, QueryType.ASK);
		return q;
	}

	@Override
	public SailTupleQuery prepareTupleQuery(QueryLanguage ql,
			String queryString, String baseURI)
	{
		SailTupleQuery q = super.prepareTupleQuery(ql, queryString, baseURI);
		insertOriginalQueryString(q, queryString, QueryType.SELECT);
		return q;
	}

	@Override
	public SailGraphQuery prepareGraphQuery(QueryLanguage ql,
			String queryString, String baseURI)
	{
		SailGraphQuery q = super.prepareGraphQuery(ql, queryString, baseURI);
		insertOriginalQueryString(q, queryString, QueryType.CONSTRUCT);
		return q;
	}

	@Override
	public SailBooleanQuery prepareBooleanQuery(QueryLanguage ql,
			String queryString, String baseURI)
	{
		SailBooleanQuery q= super.prepareBooleanQuery(ql, queryString, baseURI);
		insertOriginalQueryString(q, queryString, QueryType.ASK);
		return q;
	}

	@Override
	public Update prepareUpdate(QueryLanguage ql, String update, String baseURI)
	{
		return super.prepareUpdate(ql, update, baseURI);
	}

	private void insertOriginalQueryString(SailQuery query, String queryString, QueryType qt) {
		query.setBinding(BINDING_ORIGINAL_QUERY, SimpleValueFactory.getInstance()
				.createLiteral(queryString));
		query.setBinding(BINDING_ORIGINAL_QUERY_TYPE, SimpleValueFactory.getInstance()
				.createLiteral(qt.name()));
	}
}
