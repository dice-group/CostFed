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

import java.io.File;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

import com.fluidops.fedx.Config;
import com.fluidops.fedx.structures.Endpoint.EndpointType;


/**
 * Graph information for Sesame NativeStore initialization.<p>
 * 
 * Format:<p>
 * 
 * <code>
 * <%name%> fluid:store "NativeStore";
 * fluid:RepositoryLocation "%location%".
 * 
 * relative path (to {@link Config#getBaseDir()})
 * <http://DBpedia> fluid:store "NativeStore";
 * fluid:RepositoryLocation "data\\repositories\\native-storage.dbpedia".
 *  
 * absolute Path
 * <http://DBpedia> fluid:store "NativeStore";
 * fluid:RepositoryLocation "D:\\data\\repositories\\native-storage.dbpedia".
 * </code>
 * 
 * Note: the id is constructed from the location: repositories\\native-storage.dbpedia => native-storage.dbpedia
 * 
 * 
 * @author Andreas Schwarte
 *
 */
public class NativeGraphRepositoryInformation extends RepositoryInformation {

	public NativeGraphRepositoryInformation(Model graph, Resource repNode) {
		super(EndpointType.NativeStore);
		initialize(graph, repNode);
	}

	protected void initialize(Model graph, Resource repNode) {
		
		// name: the node's value
		setProperty("name", repNode.stringValue());
				
		// location
		Model location = graph.filter(repNode, SimpleValueFactory.getInstance().createIRI("http://fluidops.org/config#RepositoryLocation"), null);
		
		String repoLocation = location.iterator().next().getObject().stringValue();
		setProperty("location", repoLocation);
		
		// id: the name of the location
		setProperty("id", new File(repoLocation).getName());
	}
}
