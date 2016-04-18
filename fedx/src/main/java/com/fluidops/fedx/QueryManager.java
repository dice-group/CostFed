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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.Query;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.query.impl.SimpleDataset;
import org.openrdf.query.parser.ParsedOperation;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParserUtil;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import com.fluidops.fedx.exception.FedXException;
import com.fluidops.fedx.exception.FedXRuntimeException;
import com.fluidops.fedx.optimizer.Optimizer;
import com.fluidops.fedx.structures.QueryInfo;
import com.fluidops.fedx.structures.QueryType;


/**
 * QueryManager to manage queries. 
 * 
 * a) Management of running queries (abort, finish)
 * b) Factory to create queries
 * 
 * @author Andreas Schwarte
 */
public class QueryManager {

	public static Logger log = Logger.getLogger(QueryManager.class);
	
	// singleton behavior: initialized in constructor of FederationManager
	protected static QueryManager instance = null;
	protected static QueryManager getInstance() {
		if (instance == null) {
			throw new FedXRuntimeException("QueryManager not initialized, used FedXFactory methods to initialize FedX correctly.");
		}
		return instance;
	}
	
	protected final FederationManager federationManager;
	protected final Repository repo;
	protected final RepositoryConnection conn;
	protected Set<QueryInfo> runningQueries = new ConcurrentSkipListSet<QueryInfo>();
	protected Map<String, String> prefixDeclarations = new HashMap<String, String>();
	
	protected QueryManager(FederationManager federationManager, Repository repo) {
		this.federationManager = federationManager;
		this.repo = repo;
		try	{
			this.conn = repo.getConnection();
		} catch (RepositoryException e)	{
			throw new FedXRuntimeException(e);		// should never occur
		}
	}
	
	
	/**
	 * Add the query to the set of running queries, queries are identified via a unique id
	 * @param queryInfo
	 */
//	public void registerQuery(QueryInfo queryInfo) {
//		assert runningQueries.contains(queryInfo) : "Duplicate query: query " + queryInfo.getQueryID() + " is already registered.";
//		runningQueries.add(queryInfo);
//	}
	
//	public Set<QueryInfo> getRunningQueries() {
//		return new HashSet<QueryInfo>(runningQueries);
//	}
	
//	public int getNumberOfRunningQueries() {
//		return runningQueries.size();
//	}
	
	public void abortQuery(QueryInfo queryInfo) {
		synchronized (queryInfo) {
			if (!runningQueries.contains(queryInfo))
				return;		
			log.info("Aborting query " + queryInfo.getQueryID());
			throw new Error("not implemented");
//			federationManager.getScheduler().abort(queryInfo.getQueryID());
//			federationManager.getUnionScheduler().abort(queryInfo.getQueryID());
//			runningQueries.remove(queryInfo);
		}
	}
	
	public void finishQuery(QueryInfo queryInfo) {
		runningQueries.remove(queryInfo);
	}
	
	public boolean isRunning(QueryInfo queryInfo) {
		return runningQueries.contains(queryInfo);
	}
	
	/**
	 * Register a prefix declaration to be used during query evaluation. If a 
	 * known prefix is used in a query, it is substituted in the parsing step.
	 * 
	 * If prefix is null, the corresponding entry is removed.
	 * 
	 * @param prefix
	 * 				a common prefix, e.g. rdf
	 * @param namespace
	 * 				the corresponding namespace, e.g. "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
	 */
	public void addPrefixDeclaration(String prefix, String namespace) {
		if (prefix==null) {
			prefixDeclarations.remove(prefix);
			return;
		}
		
		prefixDeclarations.put(prefix, namespace);
	}
	
	
	/**
	 * Prepare a tuple query which uses the underlying federation to evaluate the query.<p>
	 * 
	 * The queryString is modified to use the declared PREFIX declarations, see 
	 * {@link Config#getPrefixDeclarations()} for details.
	 * 
	 * @param queryString
	 * @return
	 * @throws MalformedQueryException
	 */
	public static TupleQuery prepareTupleQuery(String queryString) {
				
		Query q = prepareQuery(queryString);
		if (!(q instanceof TupleQuery))
			throw new FedXRuntimeException("Query is not a tuple query: " + q.getClass());
		return (TupleQuery)q;
	}
	
	/**
	 * Prepare a tuple query which uses the underlying federation to evaluate the query.<p>
	 * 
	 * The queryString is modified to use the declared PREFIX declarations, see 
	 * {@link Config#getPrefixDeclarations()} for details.
	 * 
	 * @param queryString
	 * @return
	 * @throws MalformedQueryException
	 */
	public static GraphQuery prepareGraphQuery(String queryString) {
				
		Query q = prepareQuery(queryString);
		if (!(q instanceof GraphQuery))
			throw new FedXRuntimeException("Query is not a graph query: " + q.getClass());
		return (GraphQuery)q;
	}
	
	/**
	 * Prepare a boolean query which uses the underlying federation to evaluate the query.<p>
	 * 
	 * The queryString is modified to use the declared PREFIX declarations, see 
	 * {@link Config#getPrefixDeclarations()} for details.
	 * 
	 * @param queryString
	 * @return
	 * @throws MalformedQueryException
	 */
	public static BooleanQuery prepareBooleanQuery(String queryString) {
				
		Query q = prepareQuery(queryString);
		if (!(q instanceof BooleanQuery))
			throw new FedXRuntimeException("Unexpected query type: " + q.getClass());
		return (BooleanQuery)q;
	}
	
	
	static Pattern prefixCheck = Pattern.compile(".*PREFIX .*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
	static Pattern prefixPattern = Pattern.compile("PREFIX[ ]*(\\w*):[ ]*<(\\S*)>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
	
	/**
	 * Prepare a {@link Query} which uses the underlying federation to evaluate the SPARQL query.<p>
	 * 
	 * The queryString is modified to use the declared PREFIX declarations, see 
	 * {@link Config#getPrefixDeclarations()} for details.
	 * 
	 * @param queryString
	 * @return
	 * @throws MalformedQueryException
	 */
	public static Query prepareQuery(String queryString) {
		QueryManager qm = getInstance();
		
		if (qm.prefixDeclarations.size() > 0) {
			
			/* we have to check for prefixes in the query to not add
			 * duplicate entries. In case duplicates are present
			 * Sesame throws a MalformedQueryException
			 */
			if (prefixCheck.matcher(queryString).matches())
				queryString = qm.getPrefixDeclarationsCheck(queryString) + queryString;
			else
				queryString = qm.getPrefixDeclarations() + queryString;
		}
		
		Query q = qm.conn.prepareQuery(QueryLanguage.SPARQL, queryString);
		
		// TODO set query time
		
		return q;
	}
	
	/**
	 * Retrieve the query plan for the given query string.
	 */
	public static String getQueryPlan(String queryString) {
		
		QueryManager qm = getInstance();
				
		if (qm.prefixDeclarations.size() > 0) {
			
			/* we have to check for prefixes in the query to not add
			 * duplicate entries. In case duplicates are present
			 * Sesame throws a MalformedQueryException
			 */
			if (prefixCheck.matcher(queryString).matches())
				queryString = qm.getPrefixDeclarationsCheck(queryString) + queryString;
			else
				queryString = qm.getPrefixDeclarations() + queryString;
		}
		
		ParsedOperation query = QueryParserUtil.parseOperation(QueryLanguage.SPARQL, queryString, null);
		if (!(query instanceof ParsedQuery))
			throw new MalformedQueryException("Not a ParsedQuery: " + query.getClass());
		// we use a dummy query info object here
		QueryInfo qInfo = new QueryInfo(queryString, QueryType.SELECT);
		TupleExpr tupleExpr = ((ParsedQuery)query).getTupleExpr();
		try {
			tupleExpr = Optimizer.optimize(tupleExpr, new SimpleDataset(), EmptyBindingSet.getInstance(), FederationManager.getInstance().getStrategy(), qInfo);
			return tupleExpr.toString();
		} catch (SailException e) {
			throw new FedXException("Unable to retrieve query plan: " + e.getMessage());
		}		
	}
	
	
	/**
	 * Get the prefix declarations that have to be prepended to the query.
	 * 
	 * @return
	 */
	protected String getPrefixDeclarations() {
		StringBuilder sb = new StringBuilder();
		for (String namespace : prefixDeclarations.keySet()) {
			sb.append("PREFIX ").append(namespace).append(": <")
					.append(prefixDeclarations.get(namespace)).append(">\r\n");
		}
		return sb.toString();
	}
	
	/**
	 * Get the prefix declarations that have to be added while considering
	 * prefixes that are already declared in the query. The issue here is
	 * that duplicate declaration causes exceptions in Sesame
	 * 
	 * @param queryString
	 * @return
	 */
	protected String getPrefixDeclarationsCheck(String queryString) {
		
		Set<String> queryPrefixes = findQueryPrefixes(queryString);
		
		StringBuilder sb = new StringBuilder();
		for (String prefix : prefixDeclarations.keySet()) {
			if (queryPrefixes.contains(prefix))
				continue;	// already there, do not add
			sb.append("PREFIX ").append(prefix).append(": <")
					.append(prefixDeclarations.get(prefix)).append(">\r\n");
		}
		return sb.toString();
	}
	
	
	/**
	 * Find all prefixes declared in the query
	 * @param queryString
	 * @return
	 */
	protected static Set<String> findQueryPrefixes(String queryString) {
		
		Set<String> res = new HashSet<String>();
		
		Scanner sc = new Scanner(queryString);
		while (true) {
			while (sc.findInLine(prefixPattern)!=null) {
				MatchResult m = sc.match();
				res.add(m.group(1));
			}
			if (!sc.hasNextLine())
				break;
			sc.nextLine();
		}
		sc.close();
		return res;
	}
}
