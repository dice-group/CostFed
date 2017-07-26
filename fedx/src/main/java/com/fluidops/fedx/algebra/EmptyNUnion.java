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

import org.eclipse.rdf4j.query.algebra.TupleExpr;

import com.fluidops.fedx.structures.QueryInfo;


/**
 * Algebra construct representing an empty union.
 * 
 * @author Andreas Schwarte
 *
 */
public class EmptyNUnion extends NTuple implements EmptyResult {
	private static final long serialVersionUID = -1268373891635616169L;

	public EmptyNUnion(List<TupleExpr> args, QueryInfo queryInfo) {
		super(args, queryInfo);
	}

	@Override
	public void visit(FedXExprVisitor v) {
		v.meet(this);
	}
}
