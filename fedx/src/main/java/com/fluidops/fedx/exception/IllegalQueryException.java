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

package com.fluidops.fedx.exception;

/**
 * Thrown if something is wrong while constructing the query string.
 * 
 * @author Andreas Schwarte
 */
public class IllegalQueryException extends Exception {

	private static final long serialVersionUID = 1L;

	public IllegalQueryException() {
		super();
	}

	public IllegalQueryException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

	public IllegalQueryException(String arg0) {
		super(arg0);
	}

	public IllegalQueryException(Throwable arg0) {
		super(arg0);
	}	
}
