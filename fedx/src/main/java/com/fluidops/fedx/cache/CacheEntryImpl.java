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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.openrdf.model.Statement;

import com.fluidops.fedx.EndpointManager;
import com.fluidops.fedx.cache.Cache.StatementSourceAssurance;
import com.fluidops.fedx.exception.EntryUpdateException;
import com.fluidops.fedx.structures.Endpoint;

/**
 * Implementation for Cache Entry
 * 
 * @author Andreas Schwarte
 *
 */
public class CacheEntryImpl implements CacheEntry{

	private static final long serialVersionUID = -2078321733800349639L;
	
	
	/* map endpoint.id to the corresponding entry */
	protected Map<String, EndpointEntry> entries = new HashMap<String, EndpointEntry>();
	
	
	@Override
	public StatementSourceAssurance canProvideStatements(Endpoint endpoint) {
		EndpointEntry entry = entries.get(endpoint.getId());
		return entry == null ? StatementSourceAssurance.POSSIBLY_HAS_STATEMENTS : 
					( entry.doesProvideStatements() ? StatementSourceAssurance.HAS_REMOTE_STATEMENTS : StatementSourceAssurance.NONE );
	}

	@Override
	public List<Endpoint> getEndpoints() {
		return EndpointManager.getEndpointManager().getEndpoints(entries.keySet());
	}

	@Override
	public CloseableIteration<? extends Statement, Exception> getStatements() {
		throw new UnsupportedOperationException("This operation is not yet supported.");
	}

	@Override
	public CloseableIteration<? extends Statement, Exception> getStatements(
			Endpoint endpoint) {
		throw new UnsupportedOperationException("This operation is not yet supported.");
	}

	@Override
	public List<Endpoint> hasLocalStatements() {
		throw new UnsupportedOperationException("This operation is not yet supported.");
	}

	@Override
	public boolean hasLocalStatements(Endpoint endpoint) {
		// not yet implemented
		return false;
	}

	@Override
	public boolean isUpToDate() {
		return true;
	}

	@Override
	public void merge(CacheEntry other) throws EntryUpdateException {
		// XXX make a check if we can safely cast?
		
		CacheEntryImpl o = (CacheEntryImpl)other;
		
		for (String k : o.entries.keySet()) {
			if (!entries.containsKey(k))
				entries.put(k, o.entries.get(k));
			else {
				
				EndpointEntry _merge = o.entries.get(k);
				EndpointEntry _old = entries.get(k);
				
				_old.setCanProvideStatements( _merge.doesProvideStatements());
			}
				
		}
	}

	@Override
	public void update() throws EntryUpdateException {
		throw new UnsupportedOperationException("This operation is not yet supported.");		
	}

	@Override
	public void add(EndpointEntry endpointEntry) {
		entries.put(endpointEntry.getEndpointID(), endpointEntry);		
	}
}
