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

import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.algebra.AbstractQueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;


/**
 * Convenience AST node to print the additional values
 * of a {@link ProjectionWithBindings} node
 *  
 * @author Andreas Schwarte
 *
 */
public class AdditionalBindingsNode extends AbstractQueryModelNode
{
	private static final long serialVersionUID = 4981809543976559563L;
	private final List<Binding> additionalValues;

	public AdditionalBindingsNode(List<Binding> additionalValues) {
		super();
		this.additionalValues = additionalValues;
	}

	@Override
	public <X extends Exception> void visit(QueryModelVisitor<X> visitor) throws X {
		visitor.meetOther(this);
	}	

	@Override
	public String getSignature()
	{
		StringBuilder sb = new StringBuilder(64);
		sb.append("Additional Bindings (");
		for (int i=0; i<additionalValues.size();i++) {
			sb.append(additionalValues.get(i).getName() + "=" + additionalValues.get(i).getValue().stringValue());
			if (i<additionalValues.size()-1)
				sb.append(", ");
		}
		sb.append(")");		
		return sb.toString();		
	}
	
	public static <X extends Exception> void visit(QueryModelVisitor<X> visitor, List<Binding> additionalValues) throws X {
		new AdditionalBindingsNode(additionalValues).visit(visitor);
	}
}
