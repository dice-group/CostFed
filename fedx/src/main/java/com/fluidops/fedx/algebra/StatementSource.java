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

package com.fluidops.fedx.algebra;

import org.openrdf.query.algebra.AbstractQueryModelNode;
import org.openrdf.query.algebra.QueryModelVisitor;

/**
 * A structure representing a relevant source for some expression.
 * 
 * @author Andreas Schwarte
 *
 */
public class StatementSource extends AbstractQueryModelNode {
	private static final long serialVersionUID = 3630439660226120336L;

	public static enum StatementSourceType { LOCAL, REMOTE, REMOTE_POSSIBLY; }
	
	protected String id;
	protected StatementSourceType type;
		
	public StatementSource(String name, StatementSourceType type) {
		super();
		this.id = name;
		this.type = type;
	}

	
	@Override
	public <X extends Exception> void visit(QueryModelVisitor<X> visitor)
			throws X {
		visitor.meetOther(this);
	}

	@Override
	public String getSignature()
	{
		StringBuilder sb = new StringBuilder(64);

		sb.append(super.getSignature());
		
		sb.append(" (id=").append(id);
		
		sb.append(", type=").append(type);
		
		sb.append(")");
		
		return sb.toString();
		
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		StatementSource other = (StatementSource) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (type == null) {
			if (other.type != null)
				return false;
		} else if (!type.equals(other.type))
			return false;
		return true;
	}
	
	public String getEndpointID() {
		return id;
	}
	
	
	public boolean isLocal() {
		return type==StatementSourceType.LOCAL;
	}
	
}
