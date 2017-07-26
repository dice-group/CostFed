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

package com.fluidops.fedx.optimizer;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.rdf4j.query.algebra.StatementPattern;

import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.cache.Cache;
import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.structures.QueryInfo;


/**
 * Perform source selection during optimization 
 * 
 * @author Andreas Schwarte
 *
 */
public abstract class SourceSelection {
	public static Logger log = LoggerFactory.getLogger(SourceSelection.class);
	
	protected final List<Endpoint> endpoints;
	protected final Cache cache;
	protected final QueryInfo queryInfo;
	
	public long time = 0;
	/**
	 * Map statements to their sources.
	 */
	protected Map<StatementPattern, List<StatementSource>> stmtToSources;
	
	public SourceSelection(List<Endpoint> endpoints, Cache cache, QueryInfo queryInfo) {
		this.endpoints = endpoints;
		this.cache = cache;
		this.queryInfo = queryInfo;
		this.queryInfo.setSourceSelection(this);
	}
	
	public abstract void performSourceSelection(List<List<StatementPattern>> bgpGroups);
	
	/**
	 * Retrieve a source selection for the provided statement patterns.
	 * @return
	 */
	public Map<StatementPattern, List<StatementSource>> getStmtToSources() {
		return stmtToSources;
	}
	
	/**
	 * Retrieve a set of relevant sources for this query.
	 * @return
	 */
	public Set<Endpoint> getRelevantSources() {
		Set<Endpoint> endpoints = new HashSet<Endpoint>();
		for (List<StatementSource> sourceList : getStmtToSources().values())
			for (StatementSource source : sourceList)
				endpoints.add(queryInfo.getFedXConnection().getEndpointManager().getEndpoint(source.getEndpointID()));
		return endpoints;
	}
	
	/**
	 * Add a source to the given statement in the map (synchronized through map)
	 * 
	 * @param stmt
	 * @param source
	 */
	protected void addSource(StatementPattern stmt, StatementSource source) {
		// The list for the stmt mapping is already initialized
		List<StatementSource> sources = stmtToSources.get(stmt);
		synchronized (sources) {
			sources.add(source);
		}
	}
}


