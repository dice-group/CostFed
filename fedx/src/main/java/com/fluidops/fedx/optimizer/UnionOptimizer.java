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

package com.fluidops.fedx.optimizer;

import java.util.ArrayList;
import java.util.List;

import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Union;
import org.openrdf.query.algebra.helpers.AbstractQueryModelVisitor;

import com.fluidops.fedx.algebra.EmptyNUnion;
import com.fluidops.fedx.algebra.EmptyResult;
import com.fluidops.fedx.algebra.NUnion;
import com.fluidops.fedx.exception.OptimizationException;
import com.fluidops.fedx.structures.QueryInfo;


/**
 * Optimizer to flatten the UNION operations.
 * 
 * @author Andreas Schwarte
 *
 */
public class UnionOptimizer extends AbstractQueryModelVisitor<OptimizationException> implements FedXOptimizer{

	protected final QueryInfo queryInfo;
		
	public UnionOptimizer(QueryInfo queryInfo) {
		super();
		this.queryInfo = queryInfo;
	}

	@Override
	public void optimize(TupleExpr tupleExpr) {
		tupleExpr.visit(this);
	}
	
	
	@Override
	public void meet(Union union) {
		
		// retrieve the union arguments, also those of nested unions
		List<TupleExpr> args = new ArrayList<TupleExpr>();
		handleUnionArgs(union, args);
		
		// remove any tuple expressions that do not produce any result
		List<TupleExpr> filtered = new ArrayList<TupleExpr>(args.size());
		for (TupleExpr arg : args) {
			if (arg instanceof EmptyResult)
				continue;
			filtered.add(arg);
		}
		
		// create a NUnion having the arguments in one layer
		// however, check if we only have zero or one argument first
		if (filtered.size()==0) {
			union.replaceWith(new EmptyNUnion(args, queryInfo));
		}
		
		else if (filtered.size()==1) {
			union.replaceWith(filtered.get(0));
		}
		
		else {
			union.replaceWith( new NUnion(filtered, queryInfo) );			
		}
	}
	
	/**
	 * Add the union arguments to the args list, includes a recursion 
	 * step for nested unions.
	 * 
	 * @param union
	 * @param args
	 */
	protected void handleUnionArgs(Union union, List<TupleExpr> args) {
		
		if (union.getLeftArg() instanceof Union) {
			handleUnionArgs((Union)union.getLeftArg(), args);
		} else {
			args.add(union.getLeftArg());
		}
		
		if (union.getRightArg() instanceof Union) {
			handleUnionArgs((Union)union.getRightArg(), args);
		} else {
			args.add(union.getRightArg());
		}
	}

}
