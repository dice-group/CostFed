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

package com.fluidops.fedx.evaluation;

import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.structures.Endpoint.EndpointType;

public class TripleSourceFactory
{

	public static TripleSource tripleSourceFor(Endpoint e, EndpointType t) {
		switch (t)
		{
		case NativeStore:
			return new SailTripleSource(e);
		case SparqlEndpoint:
			return new SparqlTripleSource(e);
		case RemoteRepository:
			return new SparqlTripleSource(e);
		case Other:
			return new SparqlTripleSource(e);
		default:
			return new SparqlTripleSource(e);
		}
	}
}
