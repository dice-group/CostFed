package com.fluidops.fedx.provider;

import org.apache.log4j.Logger;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import com.fluidops.fedx.Config;
import com.fluidops.fedx.structures.Endpoint;

import info.aduna.iteration.Iterations;

/**
 * Convenience methods for {@link Endpoint} providers
 * 
 * @author Andreas Schwarte
 *
 */
public class ProviderUtil {
	private static final Logger log = Logger.getLogger(ProviderUtil.class);
	/**
	 * Checks the connection by submitting a SPARQL SELECT query:
	 * 
	 * SELECT * WHERE { ?s ?p ?o } LIMIT 1
	 * 
	 * Throws an exception if the query cannot be evaluated
	 * successfully for some reason (indicating that the 
	 * endpoint is not ok)
	 * 
	 * @param repo
	 * @throws RepositoryException
	 * @throws QueryEvaluationException
	 * @throws MalformedQueryException
	 */
	public static long checkConnectionIfConfigured(Repository repo) {
		
		if (!Config.getConfig().isValidateRepositoryConnections())
			return 0;
		
		long startTime = System.currentTimeMillis();
		
		RepositoryConnection conn = repo.getConnection();		
		try {
			TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, "SELECT * WHERE { ?s ?p ?o } LIMIT 1");
			TupleQueryResult qRes = null;
			try {
				qRes = query.evaluate();
				if (!qRes.hasNext()) {
					log.warn("No data in provided repository (" + repo + ")");
				}
				while (qRes.hasNext()) qRes.next();
				
			} finally {
				if (qRes != null) {
					Iterations.closeCloseable(qRes);
				}
			}			
		} finally {			
			conn.close();
		}
		return System.currentTimeMillis() - startTime;
	}
}
