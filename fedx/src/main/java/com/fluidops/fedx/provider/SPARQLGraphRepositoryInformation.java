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

package com.fluidops.fedx.provider;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

import com.fluidops.fedx.structures.SparqlEndpointConfiguration;
import com.fluidops.fedx.structures.Endpoint.EndpointType;


/**
 * Graph information for Sesame SPARQLRepository initialization.<p>
 * 
 * Format:<p>
 * 
 * <pre>
 * <%name%> fluid:store "SPARQLEndpoint";
 * fluid:SPARQLEndpoint "%location%"
 * 
 * <http://DBpedia> fluid:store "SPARQLEndpoint";
 * fluid:SPARQLEndpoint "http://dbpedia.org/sparql".
 * 
 * <http://NYtimes> fluid:store "SPARQLEndpoint";
 * fluid:SPARQLEndpoint "http://api.talis.com/stores/nytimes/services/sparql".
 * </pre>
 * 
 * Note: the id is constructed from the name: http://dbpedia.org/ => sparql_dbpedia.org<p>
 * 
 * 
 * The following properties can be used to define additional endpoint settings.<p>
 * 
 * <pre>
 * http://fluidops.org/config#supportsASKQueries => "true"|"false" (default: true)
 * </pre>
 * 
 * 
 * @author Andreas Schwarte
 *
 */
public class SPARQLGraphRepositoryInformation extends RepositoryInformation {

	public SPARQLGraphRepositoryInformation(Model graph, Resource repNode) {
		super(EndpointType.SparqlEndpoint);
		initialize(graph, repNode);
	}

	protected void initialize(Model graph, Resource repNode) {
		
		// name: the node's value
		setProperty("name", repNode.stringValue());
				
		// location		
		Model location = graph.filter(repNode, SimpleValueFactory.getInstance().createIRI("http://fluidops.org/config#SPARQLEndpoint"), null);
		String repoLocation = location.iterator().next().getObject().stringValue();;
		setProperty("location", repoLocation);
		
		// id: the name of the location
		String id = repNode.stringValue().replace("http://", "");
		id = "sparql_" + id.replace("/", "_");
		setProperty("id", id);
		
		// endpoint configuration (if specified)
		if (hasAdditionalSettings(graph, repNode)) {
			SparqlEndpointConfiguration c = new SparqlEndpointConfiguration();
			
			if (graph.contains(repNode, SimpleValueFactory.getInstance().createIRI("http://fluidops.org/config#supportsASKQueries"), SimpleValueFactory.getInstance().createLiteral("false")))
				c.setSupportsASKQueries(false);
			
			setEndpointConfiguration(c);
		}
	}
	
	protected boolean hasAdditionalSettings(Model graph, Resource repNode) {
		return graph.contains(repNode, SimpleValueFactory.getInstance().createIRI("http://fluidops.org/config#supportsASKQueries"), null);
	}
}
