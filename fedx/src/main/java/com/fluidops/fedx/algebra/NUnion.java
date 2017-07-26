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

import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

import com.fluidops.fedx.structures.QueryInfo;


/**
 * A tuple expression that represents an nary-Union.
 * 
 * @author Andreas Schwarte
 *
 */
public class NUnion extends NTuple {
	private static final long serialVersionUID = -8138378734514347010L;

	/**
	 * Construct an nary-tuple. Note that the parentNode of all arguments is
	 * set to this instance.
	 * 
	 * @param args
	 */
	public NUnion(List<TupleExpr> args, QueryInfo queryInfo) {
		super(args, queryInfo);
	}	
	
	@Override
	public <X extends Exception> void visit(QueryModelVisitor<X> visitor)
			throws X {
		visitor.meetOther(this);		
	}	
	
	@Override
	public NUnion clone() {
		return (NUnion)super.clone();
	}

	@Override
	public void visit(FedXExprVisitor v) {
		v.meet(this);
	}	
}
