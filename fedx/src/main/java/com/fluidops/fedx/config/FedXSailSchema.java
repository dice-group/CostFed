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

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/**
 * Defines constants for the FedX schema which is used by {@link FedXSailFactory}
 * to initialize FedX federations.
 * 
 * @author Andreas Schwarte
 */
public class FedXSailSchema {
	/** FedX schema namespace (<tt>http://www.fluidops.com/config/fedx#</tt>). */
	public static final String NAMESPACE = "http://www.fluidops.com/config/fedx#";
	
	/** <tt>http://www.fluidops.com/config/fedx#fedxConfig</tt>	 */
	public final static IRI fedXConfig;
	
	static {
		ValueFactory factory = SimpleValueFactory.getInstance();
		fedXConfig = factory.createIRI(NAMESPACE, "fedxConfig");
	}
}
