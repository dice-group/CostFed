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
 * Thrown if a filter could not be converted.
 * 
 * @author Andreas Schwarte
 */
public class FilterConversionException extends Exception {

	private static final long serialVersionUID = -5068356058040634022L;

	public FilterConversionException() {
		super();
	}

	public FilterConversionException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

	public FilterConversionException(String arg0) {
		super(arg0);
	}

	public FilterConversionException(Throwable arg0) {
		super(arg0);
	}	
}
