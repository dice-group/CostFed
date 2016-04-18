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

import org.openrdf.model.Model;
import org.openrdf.model.Resource;
import org.openrdf.model.impl.SimpleValueFactory;

import com.fluidops.fedx.structures.Endpoint.EndpointType;


/**
 * Graph information for Sesame SPARQLRepository initialization.
 * 
 * Format:
 * 
 * <code>
 * <%name%> fluid:store "RemoteRepository";
 * fluid:repositoryServer "%location%";
 * fluid:repositoryName "%name%"
 * 
 * <http://dbpedia> fluid:store "RemoteRepository";
 * fluid:repositoryServer "http://<host>/openrdf-sesame" ;
 * fluid:repositoryName "dbpedia" .
 * 
 * 
 * </code>
 * 
 * Note: the id is constructed from the name: http://dbpedia.org/ => remote_dbpedia.org
 * 
 * 
 * @author Andreas Schwarte
 *
 */
public class RemoteRepositoryGraphRepositoryInformation extends RepositoryInformation {

	public RemoteRepositoryGraphRepositoryInformation(Model graph, Resource repNode) {
		super(EndpointType.RemoteRepository);
		initialize(graph, repNode);
	}

	public RemoteRepositoryGraphRepositoryInformation(String repositoryServer, String repositoryName) {
		super("remote_" + repositoryName, "http://"+repositoryName, repositoryServer + "/" + repositoryName, EndpointType.RemoteRepository);
		setProperty("repositoryServer", repositoryServer);
		setProperty("repositoryName", repositoryName);		
	}
	
	protected void initialize(Model graph, Resource repNode) {
		
		// name: the node's value
		setProperty("name", repNode.stringValue());

		// repositoryServer / location
		Model repositoryServer = graph.filter(repNode, SimpleValueFactory.getInstance().createIRI("http://fluidops.org/config#repositoryServer"), null);
		String repoLocation = repositoryServer.iterator().next().getObject().stringValue();
		setProperty("location", repoLocation);
		setProperty("repositoryServer", repoLocation);
		
		// repositoryName
		Model repositoryName = graph.filter(repNode, SimpleValueFactory.getInstance().createIRI("http://fluidops.org/config#repositoryName"), null);
		String repoName = repositoryName.iterator().next().getObject().stringValue();
		setProperty("repositoryName", repoName);
		
		// id: the name of the location
		String id = repNode.stringValue().replace("http://", "");
		id = "remote_" + id.replace("/", "_");
		setProperty("id", id);
	}
}
