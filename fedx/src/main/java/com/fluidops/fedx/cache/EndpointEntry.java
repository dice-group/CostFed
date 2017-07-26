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

import org.eclipse.rdf4j.common.iteration.CloseableIteration;

import java.io.Serializable;

import org.eclipse.rdf4j.model.Statement;


public class EndpointEntry implements Serializable {


	private static final long serialVersionUID = -5572059274543728740L;
	
	protected final String endpointID;
	protected boolean doesProvideStatements = false;
	protected boolean hasLocalStatements = false;
	
	
	public EndpointEntry(String endpointID, boolean canProvideStatements) {
		super();
		this.endpointID = endpointID;
		this.doesProvideStatements = canProvideStatements;
	}

	public boolean doesProvideStatements() {
		return doesProvideStatements;
	}

	public CloseableIteration<? extends Statement, Exception> getStatements() {
		throw new UnsupportedOperationException("This operation is not yet supported.");
	}


	public boolean hasLocalStatements() {
		return hasLocalStatements;
	}

	public void setCanProvideStatements(boolean canProvideStatements) {
		this.doesProvideStatements = canProvideStatements;
	}


	public String getEndpointID() {
		return endpointID;
	}
	
	public String toString() {
		return getClass().getSimpleName() + " {endpointID=" + endpointID + ", doesProvideStatements=" + doesProvideStatements + ", hasLocalStatements=" + hasLocalStatements + "}";
	}
	
}
