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
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.NoConnectionReuseStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.rdf4j.IsolationLevel;
import org.eclipse.rdf4j.http.client.util.HttpClientBuilders;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;

import com.fluidops.fedx.cache.Cache;
import com.fluidops.fedx.evaluation.concurrent.ControlledWorkerScheduler;
import com.fluidops.fedx.exception.FedXException;
import com.fluidops.fedx.exception.FedXRuntimeException;
import com.fluidops.fedx.monitoring.Monitoring;
import com.fluidops.fedx.monitoring.MonitoringFactory;
import com.fluidops.fedx.statistics.Statistics;



/**
 * FedX serves as implementation of the federation layer. It implements Sesame's
 * Sail interface and can thus be used as a normal repository in a Sesame environment. The 
 * federation layer enables transparent access to the underlying members as if they 
 * were a central repository.<p>
 * 
 * For initialization of the federation and usage see {@link FederationManager}.
 * 
 * @author Andreas Schwarte
 *  
 */
public class FedX implements Sail {

	public static Logger log = LoggerFactory.getLogger(FedX.class);
	
	protected final Config config;
	protected EndpointListProvider endpointListProvider;
	protected SummaryProvider summaryProvider;
	
    protected Executor executor;
    protected ControlledWorkerScheduler scheduler;
    protected Cache cache;
    protected Statistics statistics;
    
    protected final HttpClient httpClient;
    
    static Monitoring monitoring;
    
    Properties prefixDeclarations = null;
    
	protected boolean open = false;
		
	protected FedX(Config config, Cache cache, Statistics statistics, EndpointListProvider endpointListProvider, SummaryProvider summaryProvider) {
	    this.config = config;
	    this.cache = cache;
	    this.statistics = statistics;
	    this.endpointListProvider = endpointListProvider;
	    this.summaryProvider = summaryProvider;
	    
	       // initialize httpclient parameters
        HttpClientBuilder httpClientBuilder = HttpClientBuilders.getSSLTrustAllHttpClientBuilder();
        httpClientBuilder.setMaxConnTotal(config.getMaxHttpConnectionCount());
        httpClientBuilder.setMaxConnPerRoute(config.getMaxHttpConnectionCountPerRoute());

        //httpClientBuilder.evictExpiredConnections();
        httpClientBuilder.setConnectionReuseStrategy(new NoConnectionReuseStrategy());
        //httpClientBuilder.setConnectionTimeToLive(1000, TimeUnit.MILLISECONDS);
        //httpClientBuilder.disableAutomaticRetries();

//      httpClientBuilder.setKeepAliveStrategy(new ConnectionKeepAliveStrategy(){
//
//          @Override
//          public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
//              return 0;
//          }});
        
        httpClient = httpClientBuilder.build();
        
	    synchronized (log) {
    	    if (monitoring == null) {
    	        monitoring = MonitoringFactory.createMonitoring(config);
    	    }
	    }
        
        executor = Executors.newCachedThreadPool();
        
        scheduler = new ControlledWorkerScheduler(config.getWorkerThreads(), "Evaluation Scheduler");
        if (log.isDebugEnabled()) {
            log.debug("Scheduler for async operations initialized with " + config.getWorkerThreads() + " worker threads.");
        }
        
        // initialize prefix declarations, if any
        String prefixFile = config.getPrefixDeclarations();
        if (prefixFile != null) {
            prefixDeclarations = new Properties();
            try {
                prefixDeclarations.load(new FileInputStream(new File(prefixFile)));
            } catch (IOException e) {
                throw new FedXRuntimeException("Error loading prefix properties: " + e.getMessage());
            }
        }
		open = true;
	}
	
	public Config getConfig() {
	    return config;
	}
	
	public HttpClient getHttpClient() {
	    return httpClient;
	}
	
	public Properties getPrefixDeclarations() {
	    return prefixDeclarations;
	}
	
    public Cache getCache() {
        return cache;
    }
    
    public Statistics getStatistics() {
        return statistics;
    }
    
    public Executor getExecutor() {
        return executor;
    }
    
    public static Monitoring getMonitoring() {
        return monitoring;
    }
    
    public ControlledWorkerScheduler getScheduler() {
        return scheduler;
    }
    
	@Override
	public SailConnection getConnection() throws SailException {
		return new FedXConnection(this, endpointListProvider.getEndpoints(this), summaryProvider.getSummary(this));
	}

	@Override
	public File getDataDir() {
	    log.info("Operation not supported yet. (getDataDir)");
	    return new File(config.getProperty("quetzal.fedSummaries"));
		//throw new UnsupportedOperationException("Operation not supported yet.");
	}

	@Override
	public ValueFactory getValueFactory() {
		return SimpleValueFactory.getInstance();
	}

	@Override
	public void initialize() throws SailException {
		log.debug("Initializing federation....");
		open = true;
	}

	@Override
	public boolean isWritable() throws SailException {
		return false;
	}

	@Override
	public void setDataDir(File dataDir) {
		//throw new UnsupportedOperationException("Operation not supported yet.");		
	}

	@Override
	public void shutDown() throws SailException {
		try {
		    log.info("Shutting down federation and all underlying repositories ...");
		    
		    scheduler.shutdown();
		    scheduler = null;
	        // Abort all running queries
	        shutDownInternal();
	        cache.persist();
	        
	        monitoring = null;
	        
		} catch (FedXException e) {
			throw new SailException(e);
		}		
	}
	
	/**
	 * Try to shut down all federation members.
	 * 
	 * @throws FedXException
	 * 				if not all members could be shut down
	 */
	protected void shutDownInternal() throws FedXException {
	    if (summaryProvider != null) {
	        summaryProvider.close();
	        summaryProvider = null;
	    }
	    if (endpointListProvider != null) {
	        endpointListProvider.close();
	        endpointListProvider = null;
	    }
		open = false;
	}
	
	public boolean isOpen() {
		return open;
	}

//	for sesame from 2.8
	@Override
	public IsolationLevel getDefaultIsolationLevel() {
		return org.eclipse.rdf4j.IsolationLevels.READ_COMMITTED;
	}

	@Override
	public List<IsolationLevel> getSupportedIsolationLevels() {
		return Arrays.asList(new IsolationLevel[]{org.eclipse.rdf4j.IsolationLevels.READ_COMMITTED});
	}
}
