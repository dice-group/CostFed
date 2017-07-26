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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

import com.fluidops.fedx.algebra.FedXService;
import com.fluidops.fedx.algebra.NJoin;
import com.fluidops.fedx.algebra.NTuple;
import com.fluidops.fedx.algebra.StatementTupleExpr;
import com.fluidops.fedx.exception.FedXRuntimeException;
import com.fluidops.fedx.structures.QueryInfo;

public class OptimizerUtil
{

	
	/**
	 * Flatten the join to one layer, i.e. collect all join arguments
	 * 
	 * @param join
	 * @param queryInfo
	 * @return
	 */
	public static NJoin flattenJoin(Join join, QueryInfo queryInfo) {
		List<TupleExpr> joinArgs = new ArrayList<TupleExpr>();
		collectJoinArgs(join, joinArgs);
		return new NJoin(joinArgs, queryInfo);
	}
	
	
	/**
	 * Collect join arguments by descending the query tree (recursively).
	 * 
	 * @param node
	 * @param joinArgs
	 */
	protected static void collectJoinArgs(TupleExpr node, List<TupleExpr> joinArgs) {
		
		if (node instanceof Join) {
			collectJoinArgs(((Join)node).getLeftArg(), joinArgs);
			collectJoinArgs(((Join)node).getRightArg(), joinArgs);
		} else
			joinArgs.add(node);
	}
	
	public static Collection<String> getFreeVars(TupleExpr tupleExpr) {
		if (tupleExpr instanceof StatementTupleExpr)
			return ((StatementTupleExpr)tupleExpr).getFreeVars();
		
		// determine the number of free variables in a UNION or Join
		if (tupleExpr instanceof NTuple) {
			HashSet<String> freeVars = new HashSet<String>();
			NTuple ntuple = (NTuple)tupleExpr;
			for (TupleExpr t : ntuple.getArgs())
				freeVars.addAll(getFreeVars(t));
			return freeVars;
		}


		if (tupleExpr instanceof FedXService) {
			return ((FedXService)tupleExpr).getFreeVars();
		}
		
		if (tupleExpr instanceof Service) {
			return ((Service)tupleExpr).getServiceVars();
		}
		
		// can happen in SERVICE nodes, if they cannot be optimized
		if (tupleExpr instanceof StatementPattern) {
			List<String> freeVars = new ArrayList<String>();
			StatementPattern st = (StatementPattern)tupleExpr;
			if (st.getSubjectVar().getValue()==null)
				freeVars.add(st.getSubjectVar().getName());
			if (st.getPredicateVar().getValue()==null)
				freeVars.add(st.getPredicateVar().getName());
			if (st.getObjectVar().getValue()==null)
				freeVars.add(st.getObjectVar().getName());
		}
		
		
		throw new FedXRuntimeException("Type " + tupleExpr.getClass().getSimpleName() + " not supported for cost estimation. If you run into this, please report a bug.");
		
	}
}
