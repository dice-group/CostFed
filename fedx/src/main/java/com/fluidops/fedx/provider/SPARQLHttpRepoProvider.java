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

import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.http.HTTPRepository;

import com.fluidops.fedx.exception.FedXException;
import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.structures.Endpoint.EndpointClassification;


/**
 * Provider for an Endpoint that uses a Sesame HttpRepository as underlying
 * repository. All SPARQL endpoints are considered Remote.
 * 
 * @author Andreas Schwarte
 */
public class SPARQLHttpRepoProvider implements EndpointProvider {

	@Override
	public Endpoint loadEndpoint(RepositoryInformation repoInfo) throws FedXException {

		try {
			HTTPRepository repo = new HTTPRepository(repoInfo.getLocation());
			repo.initialize();
			
			ProviderUtil.checkConnectionIfConfigured(repo);
			
			String location = repoInfo.getLocation();
			EndpointClassification epc = EndpointClassification.Remote;
					
			Endpoint res = new Endpoint(repoInfo.getId(), repoInfo.getName(), location, repoInfo.getType(), epc);
			res.setEndpointConfiguration(repoInfo.getEndpointConfiguration());
			res.setRepo(repo);
			
			return res;
		} catch (RepositoryException e) {
			throw new FedXException("Repository " + repoInfo.getId() + " could not be initialized: " + e.getMessage(), e);
		}
	}

}
