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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.openrdf.query.BindingSet;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;

import com.fluidops.fedx.cache.Cache;
import com.fluidops.fedx.evaluation.EvaluationStrategyFactory;
import com.fluidops.fedx.evaluation.FederationEvalStrategy;
import com.fluidops.fedx.evaluation.SailFederationEvalStrategy;
import com.fluidops.fedx.evaluation.SparqlFederationEvalStrategy;
import com.fluidops.fedx.evaluation.concurrent.ControlledWorkerScheduler;
import com.fluidops.fedx.evaluation.concurrent.Scheduler;
import com.fluidops.fedx.evaluation.union.ControlledWorkerUnion;
import com.fluidops.fedx.evaluation.union.SynchronousWorkerUnion;
import com.fluidops.fedx.evaluation.union.WorkerUnionBase;
import com.fluidops.fedx.exception.FedXException;
import com.fluidops.fedx.exception.FedXRuntimeException;
import com.fluidops.fedx.monitoring.Monitoring;
import com.fluidops.fedx.monitoring.MonitoringFactory;
import com.fluidops.fedx.monitoring.MonitoringUtil;
import com.fluidops.fedx.sail.FedXSailRepository;
import com.fluidops.fedx.statistics.Statistics;
import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.structures.Endpoint.EndpointClassification;
import com.fluidops.fedx.structures.Endpoint.EndpointType;
import com.fluidops.fedx.structures.QueryInfo;
import com.fluidops.fedx.util.Version;




/**
 * The FederationManager manages all modules necessary for the runtime behavior. This includes for 
 * instance the federation layer instance, cache and statistics. It is Singleton and there can only 
 * be on federation instance at a time.<p>
 * 
 * The factory {@link FedXFactory} provides various functions for initialization of FedX and should 
 * be used as the entry point for any application using FedX.<p>
 * 
 * <code>
 * Config.initialize(fedxConfig);
 * List&ltEndpoint&gt members = ...			// e.g. use EndpointFactory methods
 * FedX fed = FedXFactory.initializeFederation(members);
 * SailRepository repo = new SailRepository(fed);
 * ReositoryConnection conn = repo.getConnection();
 * 
 * // Do something with the connection, e.g. query evaluation
 * </code>
 * 
 * @author Andreas Schwarte
 *
 */
public class FederationManager {

	public static Logger log = Logger.getLogger(FederationManager.class);
	
	/**
	 * The Federation type definition: Local, Remote, Hybrid
	 * 
	 * @author Andreas Schwarte
	 */
	public static enum FederationType { LOCAL, REMOTE, HYBRID; }
	
	/**
	 * The singleton instance of the federation manager
	 */
	private static FederationManager instance = null;
	
	
	/**
	 * Initialize the Singleton {@link FederationManager} instance with the provided information. The
	 * {@link FederationManager} remains initialized until {@link #shutDown()} is invoked. 
	 * 
	 * @param members
	 * 				initialize the federation with a list of repository members, null and empty lists are allowed
	 * @param cache
	 * 				the cache instance to be used
	 * @param statistics
	 * 				the statistics instance to be used
	 */
	public static SailRepository initialize(List<Endpoint> members, Cache cache, Statistics statistics) {
		if (instance != null)
			throw new FedXRuntimeException("FederationManager already initialized.");
		
		log.info("Initializing federation manager ...");
		log.info("FedX Version Information: " + Version.getVersionString());		

		monitoring = MonitoringFactory.createMonitoring();		
		
		Executor ex = Executors.newCachedThreadPool();
		FedX federation = new FedX(members);

		FedXSailRepository repo = new FedXSailRepository(federation);
		
		instance = new FederationManager(federation, cache, statistics, ex, repo);
		instance.updateStrategy();
		instance.reset();		
		
		try	{
			repo.initialize();
		} catch (RepositoryException e)	{
			// should never occur
			throw new FedXRuntimeException(e);	
		}
		
		EndpointManager.initialize(members);
		
		if (Config.getConfig().isEnableJMX()) {
			try {
				MonitoringUtil.initializeJMXMonitoring();
			} catch (Exception e1) {
				log.error("JMX monitoring could not be initialized: " + e1.getMessage());
			}
		}
		
		// initialize prefix declarations, if any
		String prefixFile = Config.getConfig().getPrefixDeclarations();
		if (prefixFile!=null) {
			QueryManager qm = instance.getQueryManager();
			Properties props = new Properties();
			try	{
				props.load(new FileInputStream(new File(prefixFile)));
			} catch (IOException e)	{
				throw new FedXRuntimeException("Error loading prefix properties: " + e.getMessage());
			}
			
			for (String ns : props.stringPropertyNames()) {
				qm.addPrefixDeclaration(ns, props.getProperty(ns));  	// register namespace/prefix pair
			}
		}
		
		return repo;
	}
	
	
	/**
	 * Return the initialized {@link FederationManager} instance.
	 * 
	 * @return
	 * 		the federation manager
	 */
	public static FederationManager getInstance() {
		if (instance == null)
			throw new FedXRuntimeException("FederationManager has not been initialized yet, call #initialize() first.");
		return instance;
	}	
	
	
	/**
	 * Returns true if the {@link FederationManager} is initialized.
	 * 
	 * @return
	 * 		true or false;
	 */
	public static boolean isInitialized() {
		return instance!=null;
	}
	
	
	static Monitoring monitoring;
	
	public static Monitoring getMonitoringService() {
		if (!isInitialized())
			throw new IllegalStateException("Monitoring service can only be used if FedX is initialized.");
		return monitoring;
	}
	
	/* Instance variables */
	protected FedX federation;
	protected Cache cache;
	protected Statistics statistics;
	protected Executor executor;
	protected FederationEvalStrategy strategy;
	protected FederationType type;
	protected ControlledWorkerScheduler scheduler;
	//protected ControlledWorkerScheduler<BindingSet> joinScheduler;
	//protected ControlledWorkerScheduler<BindingSet> unionScheduler;
	protected QueryManager queryManager;
	
	
	
	
	private FederationManager(FedX federation, Cache cache, Statistics statistics, Executor executor, Repository repo) {
		this.federation = federation;
		this.cache = cache;
		this.statistics = statistics;
		this.executor = executor;
		QueryManager.instance = new QueryManager(this, repo);		// initialize the singleton query manager
	}
	
	
	public FedX getFederation() {
		return federation;
	}
	
	
	/**
	 * Reset the {@link Scheduler} instances, i.e. abort all running threads and create a new
	 * scheduler instance.
	 */
	public void reset() {
		if (log.isDebugEnabled()) {
			log.debug("Scheduler for async operations initialized with " + Config.getConfig().getWorkerThreads() + " worker threads.");
		}
		
		if (scheduler != null) {
			scheduler.shutdown();
		}
		scheduler = new ControlledWorkerScheduler(Config.getConfig().getWorkerThreads(), "Evaluation Scheduler");
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
	
	public Monitoring getMonitoring() {
		return monitoring;
	}
	
	public FederationEvalStrategy getStrategy() {
		return strategy;
	}
	
	public ControlledWorkerScheduler getScheduler() {
		return scheduler;
	}
	
//	public ControlledWorkerScheduler<BindingSet> getJoinScheduler() {
//		return joinScheduler;
//	}
//
//	public ControlledWorkerScheduler<BindingSet> getUnionScheduler() {
//		return unionScheduler;
//	}
	
	public FederationType getFederationType() {
		return type;
	}
	
	/**
	 * 
	 * @return the singleton query manager
	 */
	public QueryManager getQueryManager() {
		// the singleton querymanager
		return QueryManager.getInstance();
	}
	
	
	/**
	 * Add the specified endpoint to the federation. The endpoint must be initialized and
	 * the federation must not contain a member with the same endpoint location.
	 * 
	 * @param e
	 * 			the initialized endpoint
	 * @param updateStrategy
	 * 			optional parameter, to determine if strategy is to be updated, default=true
	 * 
	 * @throws FedXRuntimeException
	 * 			 if the endpoint is not initialized, or if the federation has already a member with the
	 *           same location	
	 */
	public void addEndpoint(Endpoint e, boolean ...updateStrategy) throws FedXRuntimeException {
		log.info("Adding endpoint " + e.getId() + " to federation ...");
		
		/* check if endpoint is initialized*/
		if (!e.isInitialized()) {
			try	{
				e.initialize();
			} catch (RepositoryException e1){
				throw new FedXRuntimeException("Provided endpoint was not initialized and could not be initialized: " + e1.getMessage(), e1);
			}
		}
		
		/* check for duplicate before adding: heuristic => same location */
		for (Endpoint member : federation.getMembers())
			if (member.getEndpoint().equals(e.getEndpoint()))
				throw new FedXRuntimeException("Adding failed: there exists already an endpoint with location " + e.getEndpoint() + " (eid=" + member.getId() + ")");
	
		federation.addMember(e);
		EndpointManager.getEndpointManager().addEndpoint(e);
		
		if (updateStrategy==null || updateStrategy.length==0 || (updateStrategy.length==1 && updateStrategy[0]==true))
			updateStrategy();
	}
	
	/**
	 * Add the specified endpoints to the federation and take care for updating all structures.
	 * 
	 * @param endpoints
	 * 				a list of initialized endpoints to add
	 */
	public void addAll(List<Endpoint> endpoints) {
		log.info("Adding " + endpoints.size() + " endpoints to the federation.");
		
		for (Endpoint e : endpoints) {
			addEndpoint(e, false);
		}
		
		updateStrategy();
	}
	
	/**
	 * Remove the specified endpoint from the federation.
	 * 
	 * @param e
	 * 			the endpoint
	 * @param updateStrategy
	 * 			optional parameter, to determine if strategy is to be updated, default=true
	 */
	public void removeEndpoint(Endpoint e, boolean ...updateStrategy) throws RepositoryException {
		log.info("Removing endpoint " + e.getId() + " from federation ...");
		
		/* check if e is a federation member */
		if (!federation.getMembers().contains(e))
			throw new FedXRuntimeException("Endpoint " + e.getId() + " is not a member of the current federation.");
		
		federation.removeMember(e);
		EndpointManager.getEndpointManager().removeEndpoint(e);
		e.shutDown();
		
		if (updateStrategy==null || updateStrategy.length==0 || (updateStrategy.length==1 && updateStrategy[0]==true))
			updateStrategy();
	}
	
	/**
	 * Remove all endpoints from the federation, e.g. to load a new preset. Repositories
	 * of the endpoints are shutDown, and the EndpointManager is added accordingly.
	 * 
	 * @throws RepositoryException
	 */
	public void removeAll() throws RepositoryException {
		log.info("Removing all endpoints from federation.");
		
		for (Endpoint e : new ArrayList<Endpoint>(federation.getMembers())) {
			removeEndpoint(e, false);
		}
		
		updateStrategy();
	}
	
	/**
	 * return the number of triples in the federation as string. Retrieving
	 * the size is only supported {@link EndpointType#NativeStore} and
	 * {@link EndpointType#RemoteRepository}.
	 * 
	 * If the federation contains other types of endpoints, the size is
	 * indicated as a lower bound, i.e. the string starts with a 
	 * larger sign.
	 * @return
	 */
	public String getFederationSize() {
		long size = 0;
		boolean isLowerBound = false;
		for (Endpoint e : getFederation().getMembers())
			try	{
				size += e.size();
			} catch (RepositoryException e1) {
				isLowerBound = true;
			}
		return isLowerBound ? ">" + size : Long.toString(size);
	}
	
	/**
	 * Shutdown the federation including the following operations: <p>
	 * 
	 * <ul>
	 *  <li>shut down repositories of all federation members</li>
	 *  <li>persist the cached information</li>
	 *  <li>clear the endpoint manager</li>
	 * </ul>
	 * 
	 * @throws FedXException
	 * 				if an error occurs while shutting down the federation
	 */
	public void shutDown() {
		log.info("Shutting down federation and all underlying repositories ...");
		scheduler.shutdown();
		scheduler = null;
		// Abort all running queries
		federation.shutDownInternal();
		cache.persist();
		Config.reset();
		EndpointManager.getEndpointManager().shutDown();
		instance = null;
		monitoring = null;
	}
	
	/**
	 * Create an appropriate worker union for this federation, i.e. a synchronous
	 * worker union for local federations and a multithreaded worker union
	 * for remote & hybrid federations.
	 * 
	 * @return
	 * 
	 * @see ControlledWorkerUnion
	 * @see SynchronousWorkerUnion
	 */
	public WorkerUnionBase<BindingSet> createWorkerUnion(QueryInfo queryInfo) {
		if (type == FederationType.LOCAL)
			return new SynchronousWorkerUnion<BindingSet>(queryInfo);
		return new ControlledWorkerUnion<BindingSet>(scheduler, queryInfo);
	}
	
	
	/**
	 * Update the federation evaluation strategy using the classification of endpoints 
	 * as provided by {@link Endpoint#getEndpointClassification()}:<p>
	 * 
	 * Which strategy is applied depends on {@link EvaluationStrategyFactory}.
	 * 
	 * Default strategies:
	 * <ul>
	 *  <li>local federation: {@link SailFederationEvalStrategy}</li>
	 *  <li>endpoint federation: {@link SparqlFederationEvalStrategy}</li>
	 *  <li>hybrid federation: {@link SparqlFederationEvalStrategy}</li>
	 * </ul>
	 * 
	 */
	protected void updateStrategy() {
		
		int localCount=0, remoteCount=0;
		for (Endpoint e : federation.getMembers()) {			
			if (e.getEndpointClassification()==EndpointClassification.Remote) 
				remoteCount++;
			else 
				localCount++;		
		}	
		
		boolean updated=false;
		if (remoteCount==0) {
			if (type!=FederationType.LOCAL) {
				type = FederationType.LOCAL;
				updated=true;
			}
		} else if (localCount==0) {
			if (type!=FederationType.REMOTE) {
				type = FederationType.REMOTE;
				updated=true;
			}
		} else {
			if (type!=FederationType.HYBRID) {
				type = FederationType.HYBRID;
				updated=true;
			}			
		}
		
		if (updated) {
			strategy = EvaluationStrategyFactory.getEvaluationStrategy(type);		
			log.info("Federation updated. Type: " + type + ", evaluation strategy is " + instance.strategy.getClass().getSimpleName());
		}
		
	}
	
}
