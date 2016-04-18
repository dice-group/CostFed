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

package com.fluidops.fedx.cache;

import java.util.ArrayList;
import java.util.List;

import org.openrdf.model.Resource;
import org.openrdf.model.IRI;
import org.openrdf.model.Value;
import org.openrdf.repository.RepositoryConnection;

import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.algebra.StatementSource.StatementSourceType;
import com.fluidops.fedx.cache.Cache.StatementSourceAssurance;
import com.fluidops.fedx.evaluation.TripleSource;
import com.fluidops.fedx.exception.OptimizationException;
import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.structures.SubQuery;

public class CacheUtils {

	
	/**
	 * Perform a "ASK" query for the provided statement to check if the endpoint can provide results.
	 * Update the cache with the new information.
	 * 
	 * @param cache
	 * @param endpoint
	 * @param stmt
	 * @return
	 * @throws OptimizationException
	 */
	private static boolean checkEndpointForResults(Cache cache, Endpoint endpoint, Resource subj, IRI pred, Value obj) throws OptimizationException {
		try {
			TripleSource t = endpoint.getTripleSource();
			RepositoryConnection conn = endpoint.getConn(); 

			boolean hasResults = t.hasStatements(conn, subj, pred, obj);
			
			CacheEntry entry = createCacheEntry(endpoint, hasResults);
			cache.updateEntry( new SubQuery(subj, pred, obj), entry);
			
			return hasResults;
		} catch (Exception e) {
			throw new OptimizationException("Error checking results for endpoint " + endpoint.getId() + ": " + e.getMessage(), e);
		}
	}	
	
	
	public static CacheEntry createCacheEntry(Endpoint e, boolean canProvideStatements) {
		CacheEntryImpl c = new CacheEntryImpl();
		c.add( new EndpointEntry(e.getId(), canProvideStatements));
		return c;
	}
	
	
	
	/**
	 * Checks the cache if some endpoint can provide results to the subquery. If the cache has no
	 * knowledge a remote ask query is performed and the cache is updated with appropriate information.
	 * 
	 * @param cache
	 * @param endpoints
	 * @param sq
	 * @return
	 */
	public static boolean checkCacheUpdateCache(Cache cache, List<Endpoint> endpoints, Resource subj, IRI pred, Value obj) {
		
		SubQuery q = new SubQuery(subj, pred, obj);
		
		for (Endpoint e : endpoints) {
			StatementSourceAssurance a = cache.canProvideStatements(q, e);
			if (a==StatementSourceAssurance.HAS_LOCAL_STATEMENTS || a==StatementSourceAssurance.HAS_REMOTE_STATEMENTS)
				return true;	
			if (a==StatementSourceAssurance.POSSIBLY_HAS_STATEMENTS && checkEndpointForResults(cache, e, subj, pred, obj))
				return true;
		}
		return false;
	}
	
	/**
	 * Checks the cache for relevant statement sources to the provided statement. If the cache has no
	 * knowledge ask the endpoint for further information.
	 * 
	 * @param cache
	 * @param endpoints
	 * @param stmt
	 * 
	 * @return
	 */
	public static List<StatementSource> checkCacheForStatementSourcesUpdateCache(Cache cache, List<Endpoint> endpoints, Resource subj, IRI pred, Value obj) {
		
		SubQuery q = new SubQuery(subj, pred, obj);
		List<StatementSource> sources = new ArrayList<StatementSource>(endpoints.size());
		
		for (Endpoint e : endpoints) {
			StatementSourceAssurance a = cache.canProvideStatements(q, e);

			if (a==StatementSourceAssurance.HAS_LOCAL_STATEMENTS) {
				sources.add( new StatementSource(e.getId(), StatementSourceType.LOCAL));			
			} else if (a==StatementSourceAssurance.HAS_REMOTE_STATEMENTS) {
				sources.add( new StatementSource(e.getId(), StatementSourceType.REMOTE));			
			} else if (a==StatementSourceAssurance.POSSIBLY_HAS_STATEMENTS) {
				
				// check if the endpoint has results (statistics + ask request)				
				if (CacheUtils.checkEndpointForResults(cache, e, subj, pred, obj))
					sources.add( new StatementSource(e.getId(), StatementSourceType.REMOTE));
			} 
		}
		return sources;
	}
}
