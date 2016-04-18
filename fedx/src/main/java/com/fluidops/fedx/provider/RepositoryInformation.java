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

package com.fluidops.fedx.provider;

import java.util.Properties;

import com.fluidops.fedx.structures.Endpoint.EndpointType;
import com.fluidops.fedx.structures.EndpointConfiguration;

public class RepositoryInformation {

	protected Properties props = new Properties();
	private EndpointType type = EndpointType.Other;	// the endpoint type, default Other
	private EndpointConfiguration endpointConfiguration;	// optional configuration settings for the endpoint
	
	public RepositoryInformation(String id, String name, String location, EndpointType type) {
		props.setProperty("id", id);
		props.setProperty("name", name);
		props.setProperty("location", location);
		this.type = type;
	}
	
	protected RepositoryInformation(EndpointType type) {
		this.type = type;
	}
	
	public String getId() {
		return props.getProperty("id");
	}
	
	public String getName() {
		return props.getProperty("name");
	}
	
	public String getLocation() {
		return props.getProperty("location");
	}
	
	public EndpointType getType() {
		return type;
	}
	
	/**
	 * @return the optional {@link EndpointConfiguration} or <code>null</code>
	 */
	public EndpointConfiguration getEndpointConfiguration() {
		return endpointConfiguration;
	}

	public void setEndpointConfiguration(EndpointConfiguration endpointConfiguration) {
		this.endpointConfiguration = endpointConfiguration;
	}
	
	public String get(String key) {
		return props.getProperty(key);
	}

	public String get(String key, String def) {
		return props.getProperty(key, def);
	}
	
	public void setProperty(String key, String value) {
		props.setProperty(key, value);
	}
}
