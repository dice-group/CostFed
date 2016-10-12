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

import org.openrdf.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.openrdf.query.algebra.evaluation.federation.FederatedServiceResolverImpl;
import org.openrdf.query.algebra.evaluation.federation.SPARQLFederatedService;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sparql.SPARQLRepository;

import com.fluidops.fedx.FedXFactory;
import com.fluidops.fedx.exception.FedXException;
import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.structures.EndpointConfiguration;
import com.fluidops.fedx.structures.Endpoint.EndpointClassification;
import com.fluidops.fedx.structures.SparqlEndpointConfiguration;


/**
 * Provider for an Endpoint that uses a Sesame {@link SPARQLRepository} as underlying
 * repository. All SPARQL endpoints are considered Remote.<p>
 * 
 * This {@link SPARQLProvider} implements special hard-coded endpoint configuration
 * for the DBpedia endpoint: the support for ASK queries is always set to false.
 * 
 * @author Andreas Schwarte
 */
public class SPARQLProvider implements EndpointProvider {

	@Override
	public Endpoint loadEndpoint(RepositoryInformation repoInfo) throws FedXException {

		try {
			SPARQLRepository repo = new SPARQLRepository(repoInfo.getLocation());
			repo.setHttpClient(FedXFactory.httpClient);
			repo.initialize();
			
			long rtime = ProviderUtil.checkConnectionIfConfigured(repo);
			if (rtime != 0) {
				rtime = ProviderUtil.checkConnectionIfConfigured(repo); // measure again
			}
			
			String location = repoInfo.getLocation();
			EndpointClassification epc = EndpointClassification.Remote;
			
			/*
			// register a federated service manager to deal with this endpoint
			SPARQLFederatedService federatedService = new SPARQLFederatedService(repoInfo.getLocation(), null);
			federatedService.initialize();
			FederatedServiceResolverImpl
			FederatedService Manager.getInstance().registerService(repoInfo.getName(), federatedService);
			*/
			
			Endpoint res = new Endpoint(repoInfo.getId(), repoInfo.getName(), location, repoInfo.getType(), epc);
			EndpointConfiguration ep = manipulateEndpointConfiguration(location, repoInfo.getEndpointConfiguration());
			res.setEndpointConfiguration(ep);
			res.setRepo(repo);
			res.setResponseTime(rtime);
			return res;
		} catch (RepositoryException e) {
			throw new FedXException("Repository " + repoInfo.getId() + " could not be initialized: " + e.getMessage(), e);
		}
	}

	/**
	 * Manipulate the endpoint configuration for certain common endpoints, e.g.
	 * DBpedia => does not support ASK queries
	 * 
	 * @param location
	 * @param ep
	 * @return
	 */
	private EndpointConfiguration manipulateEndpointConfiguration(String location, EndpointConfiguration ep) {
		
		// special hard-coded handling for DBpedia: does not support ASK
		if (location.equals("http://dbpedia.org/sparql")) {
			if (ep==null) {
				ep = new SparqlEndpointConfiguration();
			}
			if (ep instanceof SparqlEndpointConfiguration) {
				((SparqlEndpointConfiguration)ep).setSupportsASKQueries(false);
			}
		}
		
		return ep;
	}
}
