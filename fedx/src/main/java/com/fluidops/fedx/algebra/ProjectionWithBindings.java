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

import org.openrdf.query.Binding;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.algebra.QueryModelVisitor;
import org.openrdf.query.algebra.TupleExpr;

public class ProjectionWithBindings extends Projection implements FedXExpr {
	private static final long serialVersionUID = 4037850050188019596L;
	private final List<Binding> additionalBindings;
	
	public ProjectionWithBindings(TupleExpr arg, ProjectionElemList elements, List<Binding> additionalBindings) {
		super(arg, elements);
		this.additionalBindings = additionalBindings;
	}
	
	public List<Binding> getAdditionalBindings() {
		return this.additionalBindings;
	}
	
	@Override
	public <X extends Exception> void visitChildren(QueryModelVisitor<X> visitor)
		throws X {
		if (additionalBindings.size()>0)
			AdditionalBindingsNode.visit(visitor, additionalBindings);
		super.visitChildren(visitor);		
	}

	@Override
	public void visit(FedXExprVisitor v) {
		v.meet(this);
	}
}
