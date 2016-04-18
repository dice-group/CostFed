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

import java.util.List;

import org.openrdf.query.algebra.AbstractQueryModelNode;
import org.openrdf.query.algebra.QueryModelVisitor;


/**
 * Convenience AST node to print the local variables of
 * {@link StatementTupleExpr} instances
 * 
 * @author Andreas Schwarte
 *
 */
public class LocalVarsNode extends AbstractQueryModelNode
{
	private static final long serialVersionUID = -8003551844890338374L;
	private final List<String> localVars;

	public LocalVarsNode(List<String> localVars) {
		super();
		this.localVars = localVars;
	}

	@Override
	public <X extends Exception> void visit(QueryModelVisitor<X> visitor) throws X {
		visitor.meetOther(this);
	}	

	@Override
	public String getSignature()
	{
		StringBuilder sb = new StringBuilder(64);
		sb.append("LocalVars (");
		for (int i=0; i<localVars.size();i++) {
			sb.append(localVars.get(i));
			if (i<localVars.size()-1)
				sb.append(", ");
		}
		sb.append(")");		
		return sb.toString();		
	}
	
	public static <X extends Exception> void visit(QueryModelVisitor<X> visitor, List<String> localVars) throws X {
		new LocalVarsNode(localVars).visit(visitor);
	}
}
