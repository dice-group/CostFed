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

package com.fluidops.fedx.config;

import org.eclipse.rdf4j.sail.config.AbstractSailImplConfig;


/**
 * FedX Sail config to be used for repository initialization using
 * the {@link FedXSailFactory}.
 * 
 * @author Andreas Schwarte
 */
public class FedXSailConfig extends AbstractSailImplConfig {

	/** the location of the fedx configuration */
	private String fedxConfig;
	
	
	public FedXSailConfig() {
		super(FedXSailFactory.SAIL_TYPE);
	}
	
	public FedXSailConfig(String fedxConfig) {
		this();
		this.fedxConfig = fedxConfig;
	}

	/**
	 * @return
	 * 		the location of the FedX configuration
	 */
	public String getFedxConfig() {
		return fedxConfig;
	}

	/**
	 * Set the location of the FedX configuration
	 */
	public void setFedxConfig(String fedxConfig) {
		this.fedxConfig = fedxConfig;
	}	
	
}
