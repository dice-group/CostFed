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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.openrdf.IsolationLevel;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.SimpleValueFactory;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import com.fluidops.fedx.exception.ExceptionUtil;
import com.fluidops.fedx.exception.FedXException;
import com.fluidops.fedx.structures.Endpoint;



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

	public static Logger log = Logger.getLogger(FedX.class);
	
	protected final List<Endpoint> members = new ArrayList<Endpoint>();
	protected boolean open = false;
		
	protected FedX() {
		this(null);
	}
	
	protected FedX(List<Endpoint> endpoints) {
		if (endpoints != null)
			for (Endpoint e : endpoints)
				addMember(e);
		open = true;
	}
	
	/**
	 * Add a member to the federation (internal)
	 * @param endpoint
	 */
	protected void addMember(Endpoint endpoint) {
		members.add(endpoint);
	}
	
	/**
	 * Remove a member from the federation (internal)
	 * 
	 * @param endpoint
	 * @return
	 */
	public boolean removeMember(Endpoint endpoint) {
		return members.remove(endpoint);
	}	
	
	
	@Override
	public SailConnection getConnection() throws SailException {
		return new FedXConnection(this);
	}

	@Override
	public File getDataDir() {
		throw new UnsupportedOperationException("Operation not supported yet.");
	}

	@Override
	public ValueFactory getValueFactory() {
		return SimpleValueFactory.getInstance();
	}

	@Override
	public void initialize() throws SailException {
		log.debug("Initializing federation....");
		for (Endpoint member : members) {
			try {
				member.initialize();
			} catch (RepositoryException e) {
				log.error("Initialization of endpoint " + member.getId() + " failed: " + e.getMessage());
				throw new SailException(e);
			}
		}	
		open = true;
	}

	@Override
	public boolean isWritable() throws SailException {
		return false;
	}

	@Override
	public void setDataDir(File dataDir) {
		throw new UnsupportedOperationException("Operation not supported yet.");		
	}

	@Override
	public void shutDown() throws SailException {
		try {
			FederationManager.getInstance().shutDown();
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
		List<Exception> errors = new ArrayList<Exception>(2);
		for (Endpoint member : members) {
			try {
				member.shutDown();
			} catch (Exception e) {
				log.error( ExceptionUtil.getExceptionString("Error shutting down endpoint " + member.getId(), e) );
				errors.add(e);
			}
		}
		
		if (errors.size()>0)
			throw new FedXException("At least one federation member could not be shut down.", errors.get(0));
		
		open = false;
	}
	
	public List<Endpoint> getMembers() {
		return new ArrayList<Endpoint>(members);
	}	
	
	public boolean isOpen() {
		return open;
	}

//	for sesame from 2.8
	@Override
	public IsolationLevel getDefaultIsolationLevel() {
		return org.openrdf.IsolationLevels.READ_COMMITTED;
	}

	@Override
	public List<IsolationLevel> getSupportedIsolationLevels() {
		return Arrays.asList(new IsolationLevel[]{org.openrdf.IsolationLevels.READ_COMMITTED});
	}
}
