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

import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.eclipse.rdf4j.sail.nativerdf.NativeStoreExt;

import com.fluidops.fedx.Config;
import com.fluidops.fedx.exception.FedXException;
import com.fluidops.fedx.exception.FedXRuntimeException;
import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.structures.Endpoint.EndpointClassification;
import com.fluidops.fedx.util.FileUtil;


/**
 * Provider for an Endpoint that uses a Sesame {@link NativeStore} as underlying
 * repository. For optimization purposes the NativeStore is wrapped within a
 * {@link NativeStoreExt} to allow for evaluation of prepared queries without
 * prior optimization. Note that NativeStores are always classified as 'Local'.
 * 
 * @author Andreas Schwarte
 */
public class NativeStoreProvider implements EndpointProvider {

    final Config config;
    public NativeStoreProvider(Config config) {
        this.config = config;
    }
    
	@Override
	public Endpoint loadEndpoint(RepositoryInformation repoInfo) throws FedXException {
		
		File store = FileUtil.getFileLocation(config, repoInfo.getLocation());
		
		if (!store.exists()){
			throw new FedXRuntimeException("Store does not exist at '" + repoInfo.getLocation() + ": " + store.getAbsolutePath() + "'.");
		}
		
		try {
			NativeStore ns = new NativeStoreExt(store);
			SailRepository repo = new SailRepository(ns);
			repo.initialize();
			
			ProviderUtil.checkConnectionIfConfigured(config, repo);
			
			Endpoint res = new Endpoint(repoInfo.getId(), repoInfo.getName(), repoInfo.getLocation(), repoInfo.getType(), EndpointClassification.Local);
			res.setEndpointConfiguration(repoInfo.getEndpointConfiguration());
			res.setRepo(repo);
			
			/*
			// register a federated service manager to deal with this endpoint
			SAILFederatedService federatedService = new SAILFederatedService(res);
			federatedService.initialize();
			FederatedServiceManager.getInstance().registerService(repoInfo.getName(), federatedService);
			*/
			
			return res;
		} catch (RepositoryException e) {
			throw new FedXException("Repository " + repoInfo.getId() + " could not be initialized: " + e.getMessage(), e);
		}
	}
}
