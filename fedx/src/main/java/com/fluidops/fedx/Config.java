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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.fluidops.fedx.cache.MemoryCache;
import com.fluidops.fedx.evaluation.FederationEvalStrategy;
import com.fluidops.fedx.evaluation.SailFederationEvalStrategy;
import com.fluidops.fedx.evaluation.SparqlFederationEvalStrategy;
import com.fluidops.fedx.evaluation.SparqlFederationEvalStrategyWithValues;
import com.fluidops.fedx.evaluation.concurrent.ControlledWorkerScheduler;
import com.fluidops.fedx.exception.FedXException;
import com.fluidops.fedx.exception.FedXRuntimeException;
import com.fluidops.fedx.monitoring.QueryLog;
import com.fluidops.fedx.monitoring.QueryPlanLog;
import com.fluidops.fedx.provider.ProviderUtil;


/**
 * Configuration properties for FedX based on a properties file. Prior to using this configuration
 * {@link #initialize(String)} must be invoked with the location of the properties file.
 * 
 * @author Andreas Schwarte
 *
 */
public class Config {

	protected static Logger log = Logger.getLogger(Config.class);
	
	private static Config instance = null;
	
	public static Config getConfig() {
		if (instance==null)
			throw new FedXRuntimeException("Config not initialized. Call Config.initialize() first.");
		return instance;
	}
	
	protected static void reset() {
		String ecls = getExtensionClass();
		if (ecls != null) {
			try {
				Class.forName(ecls).getMethod("reset").invoke(null);
			} catch (Exception e) {
				throw new FedXException("Can't reset a config exntension class " + ecls, e);
			} 
		}
		instance = null;
	}
	
	/**
	 * Initialize the configuration with the specified properties file.
	 * 
	 * @param fedxConfig
	 * 			the optional location of the properties file. If not specified the default configuration is used.
	 * 
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws IllegalArgumentException
	 */
	public static void initialize(String ...fedxConfig) {
		if (instance != null) {
			throw new FedXRuntimeException("Config is already initialized.");
		}
		instance = new Config();
		String cfg = fedxConfig != null && fedxConfig.length == 1 ? fedxConfig[0] : null;
		instance.init(cfg);
		
		String ecls = getExtensionClass();
		if (ecls != null) {
			try {
				Class.forName(ecls).getMethod("initialize").invoke(null);
			} catch (Exception e) {
				throw new FedXException("Can't initialize a config exntension class " + ecls, e);
			} 
		}
	}
	

	
	private Properties props;
	private Config() {
		props = new Properties();
	}
	
	private void init(String configFile) throws FedXException {
		if (configFile == null) {
			log.warn("No configuration file specified. Using default config initialization.");
			return;
		}
		log.info("FedX Configuration initialized from file '" + configFile + "'.");
		FileInputStream in = null;
		try {
			in = new FileInputStream(configFile);
			props.load( in );
		} catch (Exception e) {
			throw new FedXException("Failed to initialize FedX configuration with " + configFile + ": " + e.getMessage());
		} finally {
			if (in != null) try { in.close(); } catch (Throwable ignore) {}
		}
	}
	
	public static String getProperty(String propertyName) {
		return getConfig().props.getProperty(propertyName);
	}
	
	public static String getProperty(String propertyName, String def) {
		return getConfig().props.getProperty(propertyName, def);
	}
	
	/**
	 * the base directory for any location used in fedx, e.g. for repositories
	 * 
	 * @return
	 */
	public String getBaseDir() {
		return props.getProperty("baseDir", "");
	}
	
	/**
	 * The location of the dataConfig.
	 * 
	 * @return
	 */
	public String getDataConfig() {
		return props.getProperty("dataConfig");
	}
	
	
	/**
	 * The location of the cache, i.e. currently used in {@link MemoryCache}
	 * 
	 * @return
	 */
	public String getCacheLocation() {
		return props.getProperty("cacheLocation", "cache.db");
	}
	
	public static int getMaxHttpConnectionCount() {
		return Integer.parseInt(getConfig().props.getProperty("maxHttpConnectionCount", "1000"));
	}
	
	public static int getMaxHttpConnectionCountPerRoute() {
		return Integer.parseInt(getConfig().props.getProperty("maxHttpConnectionCountPerRoute", "200"));
	}
	
	/**
	 * The number of worker threads used in the {@link ControlledWorkerScheduler}
	 * for any operations. Default is 20.
	 * 
	 * @return
	 */
	public int getWorkerThreads() {
		return Integer.parseInt(props.getProperty("workerThreads", "20"));
	}
	
	/**
	 * The block size for a bound join, i.e. the number of bindings that are integrated
	 * in a single subquery. Default is 15.
	 * 
	 * @return
	 */
	public int getBoundJoinBlockSize() {
		return Integer.parseInt( props.getProperty("boundJoinBlockSize", "15"));
	}
	
	/**
	 * Get the maximum query time in seconds used for query evaluation. Applied in CLI
	 * or in general if {@link QueryManager} is used to create queries.<p>
	 * 
	 * Set to 0 to disable query timeouts.
	 * 
	 * @return
	 */
	public int getEnforceMaxQueryTime() {
		return Integer.parseInt( props.getProperty("enforceMaxQueryTime", "30"));
	}
	
	/**
	 * Flag to enable/disable monitoring features. Default=false.
	 * 
	 * @return
	 */
	public boolean isEnableMonitoring() {
		return Boolean.parseBoolean( props.getProperty("enableMonitoring", "false"));	
	}
	
	/**
	 * Flag to enable/disable JMX monitoring. Default=false
	 * 
	 * @return
	 */
	public boolean isEnableJMX() {
		return Boolean.parseBoolean( props.getProperty("monitoring.enableJMX", "false"));	
	}
	
	/**
	 * Flag to enable/disable query plan logging via {@link QueryPlanLog}. Default=false
	 * The {@link QueryPlanLog} facility allows to retrieve the query execution plan
	 * from a variable local to the executing thread.
	 * 
	 * @return
	 */
	public boolean isLogQueryPlan() {
		return Boolean.parseBoolean( props.getProperty("monitoring.logQueryPlan", "false"));	
	}
	
	/**
	 * Flag to enable/disable query logging via {@link QueryLog}. Default=false
	 * The {@link QueryLog} facility allows to log all queries to a file. See 
	 * {@link QueryLog} for details. 
	 * 
	 * @return
	 */
	public boolean isLogQueries() {
		return Boolean.parseBoolean( props.getProperty("monitoring.logQueries", "false"));	
	}
	
	/**
	 * Returns the path to a property file containing prefix declarations as 
	 * "namespace=prefix" pairs (one per line).<p> Default: no prefixes are 
	 * replaced. Note that prefixes are only replaced when using the CLI
	 * or the {@link QueryManager} to create/evaluate queries.
	 * 
	 * Example:
	 * 
	 * <code>
	 * foaf=http://xmlns.com/foaf/0.1/
	 * rdf=http://www.w3.org/1999/02/22-rdf-syntax-ns#
	 * =http://mydefaultns.org/
	 * </code>
	 * 			
	 * @return
	 */
	public String getPrefixDeclarations() {
		return props.getProperty("prefixDeclarations");
	}
	
	/**
	 * Returns the fully qualified class name of the SourceSelection implementation. 
	 * 
	 * 
	 * @return
	 */
	public static String getExtensionClass() {
		return getConfig().props.getProperty("extensionClass", null);
	}
	
	/**
	 * Returns the fully qualified class name of the SourceSelection implementation. 
	 * 
	 * 
	 * @return
	 */
	public static String getStatementGroupOptimizerClass() {
		return getConfig().props.getProperty("statementGroupOptimizerClass", com.fluidops.fedx.optimizer.StatementGroupOptimizer.class.getName());
	}
	
	/**
	 * Returns the fully qualified class name of the SourceSelection implementation. 
	 * 
	 * 
	 * @return
	 */
	public static String getSourceSelectionClass() {
		return getConfig().props.getProperty("sourceSelectionClass", com.fluidops.fedx.optimizer.DefaultSourceSelection.class.getName());
	}
	
	/**
	 * Returns the fully qualified class name of the {@link FederationEvalStrategy} implementation
	 * that is used in the case of SAIL implementations, e.g. for native stores. 
	 * 
	 * Default {@link SailFederationEvalStrategy}
	 * 
	 * @return
	 */
	public static String getSailEvaluationStrategy() {
		return getConfig().props.getProperty("sailEvaluationStrategy", SailFederationEvalStrategy.class.getName());
	}
	
	/**
	 * Returns the fully qualified class name of the {@link FederationEvalStrategy} implementation
	 * that is used in the case of SPARQL implementations, e.g. SPARQL repository or remote repository. 
	 * 
	 * Default {@link SparqlFederationEvalStrategy}
	 * 
	 * Alternative implementation: {@link SparqlFederationEvalStrategyWithValues}
	 * 
	 * @return
	 */
	public static String getSPARQLEvaluationStrategy() {
		return getConfig().props.getProperty("sparqlEvaluationStrategy", SparqlFederationEvalStrategy.class.getName());
	}
	
	/**
	 * Returns a flag indicating whether vectored evaluation using the VALUES clause shall
	 * be applied for SERVICE expressions. 
	 * 
	 * Default: false
	 * 
	 * Note: for todays endpoints it is more efficient to disable vectored evaluation of SERVICE.
	 * 
	 * @return
	 */
	public boolean getEnableServiceAsBoundJoin() {
		return Boolean.parseBoolean(props.getProperty("optimizer.enableServiceAsBoundJoin", "false"));
	}
	
	/**
	 * If enabled, repository connections are validated by {@link ProviderUtil#checkConnectionIfConfigured(org.openrdf.repository.Repository)}
	 * prior to adding the endpoint to the federation. If validation fails, an error is thrown to the user.
	 * 
	 * @return
	 */
	public boolean isValidateRepositoryConnections() {
		return Boolean.parseBoolean( props.getProperty("validateRepositoryConnections", "true"));
	}
	
	/**
	 * The debug mode for worker scheduler, the scheduler prints usage stats regularly
	 * if enabled
	 * 
	 * @return
	 * 		false
	 */
	public boolean isDebugWorkerScheduler() {
		return Boolean.parseBoolean( props.getProperty("debugWorkerScheduler", "false"));
	}
	
	/**
	 * The debug mode for query plan. If enabled, the query execution plan is
	 * printed to stdout
	 * 
	 * @return
	 * 		false
	 */
	public boolean isDebugQueryPlan() {
		return Boolean.parseBoolean( props.getProperty("debugQueryPlan", "false"));
	}
	
	/**
	 * Set some property at runtime
	 * @param key
	 * @param value
	 */
	public void set(String key, String value) {
		props.setProperty(key, value);
	}
}
