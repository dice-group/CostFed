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

package com.fluidops.fedx;

import java.io.File;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.Sail;

import com.fluidops.fedx.cache.Cache;
import com.fluidops.fedx.cache.MemoryCache;
import com.fluidops.fedx.exception.FedXException;
import com.fluidops.fedx.exception.FedXRuntimeException;
import com.fluidops.fedx.monitoring.MonitoringUtil;
import com.fluidops.fedx.sail.FedXSailRepository;
import com.fluidops.fedx.statistics.Statistics;
import com.fluidops.fedx.statistics.StatisticsImpl;
import com.fluidops.fedx.util.Version;

/**
 * FedX initialization factory methods for convenience: methods initialize the 
 * {@link FederationManager} and all required FedX structures. See {@link FederationManager}
 * for some a code snippet.
 * 
 * @author Andreas Schwarte
 *
 */
public class FedXFactory {

	protected static Logger log = LoggerFactory.getLogger(FedXFactory.class);
	
	/**
	 * Initialize the federation with the provided sparql endpoints. 
	 * 
	 * NOTE: {@link Config#initialize()} needs to be invoked before.
	 * 
	 * @param dataConfig
	 * 				the location of the data source configuration
	 * 
	 * @return
	 * 			the initialized FedX federation {@link Sail} wrapped in a {@link SailRepository}
	 * 
	 * @throws Exception
	 */
	public static FedXSailRepository initializeSparqlFederation(Config config, List<String> sparqlEndpoints) {
		return initializeFederation(config, new DefaultEndpointListProvider(sparqlEndpoints), config.getSummaryProvider(), config.getCacheLocation());
	}
	

	
	/**
	 * Initialize the federation with a specified data source configuration file (*.ttl). Federation members are 
	 * constructed from the data source configuration. Sample data source configuration files can be found in the documentation.
	 * 
	 * NOTE: {@link Config#initialize()} needs to be invoked before.
	 * 
	 * @param dataConfig
	 * 				the location of the data source configuration
	 * 
	 * @return
	 * 			the initialized FedX federation {@link Sail} wrapped in a {@link SailRepository}
	 * 
	 * @throws Exception
	 */
	public static FedXSailRepository initializeFederation(Config config, String dataConfig) throws Exception {
		log.info("Loading federation members from dataConfig " + dataConfig + ".");
		return initializeFederation(config, new EndpointListFileProvider(new File(dataConfig)), config.getSummaryProvider(), config.getCacheLocation());
	}
	
    public static FedXSailRepository initializeFederation(Config config) throws Exception {
        return initializeFederation(config, config.getEndpointListProvider(), config.getSummaryProvider(), config.getCacheLocation());
    }
	
	/**
	 * Initialize the federation by providing information about the fedx configuration (c.f. {@link Config}
	 * for details on configuration parameters) and additional endpoints to add. The fedx configuration
	 * can provide information about the dataConfig to be used which may contain the default federation 
	 * members.
	 * 
	 * The Federation employs a {@link MemoryCache} which is located at {@link Config#getCacheLocation()}.
	 *  
	 * @param fedxConfig
	 * 			the location of the fedx configuration
	 * @param additionalEndpoints
	 * 			additional endpoints to be added, may be null or empty
	 *  
	 * @return
	 * 			the initialized FedX federation {@link Sail} wrapped in a {@link SailRepository}
	 * 
	 * @throws Exception
	 */
	public static FedXSailRepository initializeFederation(String fedxConfig, EndpointListProvider endpointListProvider) throws FedXException {
		if (!(new File(fedxConfig).exists()))
			throw new FedXException("FedX Configuration cannot be accessed at " + fedxConfig);
		Config config = new Config(fedxConfig);
		return initializeFederation(config, endpointListProvider, config.getSummaryProvider(), config.getCacheLocation());
	}
	
	public static FedXSailRepository initializeFederation(String fedxConfig) throws Exception {
        if (!(new File(fedxConfig).exists()))
            throw new FedXException("FedX Configuration cannot be accessed at " + fedxConfig);
        Config config = new Config(fedxConfig);
        return initializeFederation(config);
    }
	
	/**
	 * Initialize the federation by providing the endpoints to add. The fedx configuration can provide information
	 * about the dataConfig to be used which may contain the default federation  members.<p>
	 * 
	 * NOTE: {@link Config#initialize()} needs to be invoked before.
	 * 
	 * @param additionalEndpoints
	 * 			additional endpoints to be added, may be null or empty
	 *  
	 * @return
	 * 			the initialized FedX federation {@link Sail} wrapped in a {@link SailRepository}
	 * 
	 * @throws Exception
	 */
	public static FedXSailRepository initializeFederation(Config config, EndpointListProvider endpointListProvider) throws FedXException {
		return initializeFederation(config, endpointListProvider, config.getSummaryProvider(), config.getCacheLocation());
	}
	
	
	/**
	 * Helper method to initialize the federation with a {@link MemoryCache}.
	 * 
	 * @param members
	 * @param cacheLocation
	 * @return
	 */
	private static FedXSailRepository initializeFederation(Config config, EndpointListProvider endpointListProvider, SummaryProvider summaryProvider, String cacheLocation) throws FedXException {

		Cache cache = new MemoryCache(cacheLocation);
		cache.initialize();
		Statistics statistics = new StatisticsImpl();
		
        log.info("FedX Version Information: " + Version.getVersionString());        

        FedX federation = new FedX(config, cache, statistics, endpointListProvider, summaryProvider);

        FedXSailRepository repo = new FedXSailRepository(federation);
        
        try {
            repo.initialize();
        } catch (RepositoryException e) {
            // should never occur
            throw new FedXRuntimeException(e);  
        }
        
        if (config.isEnableJMX()) {
            try {
                MonitoringUtil.initializeJMXMonitoring(federation);
            } catch (Exception e1) {
                log.error("JMX monitoring could not be initialized: " + e1.getMessage());
            }
        }
        
        return repo;
	}
}
