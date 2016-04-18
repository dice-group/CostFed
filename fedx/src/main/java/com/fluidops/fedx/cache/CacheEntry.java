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

import info.aduna.iteration.CloseableIteration;

import java.io.Serializable;
import java.util.List;

import org.openrdf.model.Statement;

import com.fluidops.fedx.cache.Cache.StatementSourceAssurance;
import com.fluidops.fedx.exception.EntryUpdateException;
import com.fluidops.fedx.structures.Endpoint;

/**
 * Interface for a CacheEntry
 * 
 * @author Andreas Schwarte
 *
 */
public interface CacheEntry extends Serializable {

	/**
	 * Returns HAS_STATEMENTS, if this endpoint can provide results, NONE in the contrary case.
	 * 
	 * Initially it is pessimistically assumed that each endpoint can provide result. Hence,
	 * if some endpoint is not known to the cache, the cache return POSSIBLY_HAS_STATEMENTS
	 * 
	 * @param endpoint
	 * @return
	 */
	public StatementSourceAssurance canProvideStatements(Endpoint endpoint);
	
	
	/**
	 * Returns a list of endpoints for which this cache result has any locally available data.
	 * 
	 * @return
	 * 		a list of endpoints or the empty list
	 */
	public List<Endpoint> hasLocalStatements();
	
	/**
	 * Returns true iff this cache result has any local available data for the specified endpoint.
	 * 
	 * @param endpoint
	 * @return
	 */
	public boolean hasLocalStatements(Endpoint endpoint);
	
	
	/**
	 * Return all results available for this cache entry.
	 *  
	 * @return
	 */
	public CloseableIteration<? extends Statement, Exception> getStatements();
	
	
	/**
	 * Return all results available for the specified endpoint.
	 * 
	 * @param endpoint
	 * @return
	 */
	public CloseableIteration<? extends Statement, Exception> getStatements(Endpoint endpoint);
	
	
	/**
	 * Return the endpoints that are mirrored in the cache result
	 * 
	 * @return
	 */
	public List<Endpoint> getEndpoints();
	
	
	/**
	 * Ask if this CacheResult is up2date
	 * 
	 * @return
	 */
	public boolean isUpToDate();
	
	
	/**
	 * Update this cache result, e.g. retrieve contents from web source (Optional Operation)
	 * 
	 * @throws EntryUpdateException
	 */
	public void update() throws EntryUpdateException;
	
	
	
	public void add(EndpointEntry endpointEntry);
	
	
	/**
	 * Update this cache entry and merge data with the specified item.
	 * 
	 * @param other
	 * @throws EntryUpdateException
	 */
	public void merge(CacheEntry other) throws EntryUpdateException;
}
