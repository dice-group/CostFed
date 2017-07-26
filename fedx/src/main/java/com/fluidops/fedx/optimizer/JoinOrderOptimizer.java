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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

import com.fluidops.fedx.algebra.ExclusiveGroup;
import com.fluidops.fedx.algebra.ExclusiveStatement;
import com.fluidops.fedx.algebra.FedXService;
import com.fluidops.fedx.algebra.NJoin;
import com.fluidops.fedx.algebra.NUnion;
import com.fluidops.fedx.algebra.StatementSourcePattern;
import com.fluidops.fedx.util.QueryStringUtil;


/**
 * Join Order Optimizer
 * 
 * Group -> Statements according to number of free Variables
 * 
 * Additional Heuristics:
 *  - ExclusiveGroups are cheaper than any other subquery
 *  - owned statements are cheaper if they have a single free variable
 * 
 * @author Andreas Schwarte
 *
 */
public class JoinOrderOptimizer {

	public static Logger log = LoggerFactory.getLogger(JoinOrderOptimizer.class);
	
	public static List<TupleExpr> optimizeJoinOrder(List<TupleExpr> joinArgs) {
		
		List<TupleExpr> optimized = new ArrayList<TupleExpr>(joinArgs.size());
		List<TupleExpr> left = new LinkedList<TupleExpr>(joinArgs);
		Set<String> joinVars = new HashSet<String>();
		
		while (!left.isEmpty())	{
			
			TupleExpr item = left.get(0);
			
			double minCost = Double.MAX_VALUE;
			for (TupleExpr tmp : left) {
				
				double currentCost = estimateCost(tmp, joinVars);
				if (currentCost<minCost) {
					item = tmp;
					minCost = currentCost;
				}
			}
			
			joinVars.addAll(OptimizerUtil.getFreeVars(item));
			if (log.isTraceEnabled())
				log.trace("Cost of " + item.getClass().getSimpleName() + " is determined as " + minCost);
			optimized.add(item);
			left.remove(item);
		}
		
		return optimized;
	}
	
	public static List<ExclusiveStatement> optimizeGroupOrder(List<ExclusiveStatement> groupStmts) {
		
		// in this case we do not have to order at all
		if (groupStmts.size()==1)
			return groupStmts;
		
		List<ExclusiveStatement> optimized = new ArrayList<ExclusiveStatement>(groupStmts.size());
		List<ExclusiveStatement> left = new LinkedList<ExclusiveStatement>(groupStmts);
		Set<String> joinVars = new HashSet<String>();
		
		while (!left.isEmpty())	{
			
			ExclusiveStatement item = left.get(0);
			
			double minCost = Double.MAX_VALUE;
			for (ExclusiveStatement tmp : left) {
				
				double currentCost = estimateCost(tmp, joinVars);
				if (currentCost<minCost) {
					item = tmp;
					minCost = currentCost;
				}
			}
			
			joinVars.addAll(OptimizerUtil.getFreeVars(item));
			optimized.add(item);
			left.remove(item);
		}
		
		return optimized;
	}

	
	protected static double estimateCost(TupleExpr tupleExpr, Set<String> joinVars) {
		
		if (tupleExpr instanceof StatementSourcePattern)
			return estimateCost((StatementSourcePattern)tupleExpr, joinVars);
		if (tupleExpr instanceof ExclusiveStatement)
			return estimateCost((ExclusiveStatement)tupleExpr, joinVars);
		if (tupleExpr instanceof ExclusiveGroup)
			return estimateCost((ExclusiveGroup)tupleExpr, joinVars);
		if (tupleExpr instanceof NJoin)
			return estimateCost((NJoin)tupleExpr, joinVars);
		if (tupleExpr instanceof NUnion)
			return estimateCost((NUnion)tupleExpr, joinVars);
		if (tupleExpr instanceof FedXService) 
			return estimateCost( (FedXService)tupleExpr, joinVars);
		
		
		log.warn("No cost estimation for " + tupleExpr.getClass().getSimpleName() + " available.");
		
		return 1000d;
	}
	
	protected static double estimateCost(ExclusiveGroup group, Set<String> joinVars) {
	
		// special heuristic: if not ordered at first place (i.e. there is a join var)
		// use the same counting technique as for others
		if (joinVars.size()>0) {
			int count = 0;
			for (String var : group.getFreeVars())
				if (!joinVars.contains(var))
					count++;
			return 100 + (count / group.getStatements().size() );
		}	
		
		// heuristic: a group has a selective statement, if one statement has 0 or 1 free variables
		// if it has a selective statement, the group is executed as early as possible
		boolean hasSelectiveStatement = false;

		// TODO maybe add additional cost factor for ?x rdf:type :someType
		
		for (ExclusiveStatement stmt : group.getStatements()) {
			if (stmt.getFreeVarCount()<=1) {
				hasSelectiveStatement = true;
			}			
		}
				
		double additionalCost = 0;
		
		// add some slight cost factor, if the subject in the query is not the same
		additionalCost += computeAdditionPatternCost(group.getStatements());
		
		if (!hasSelectiveStatement)
			additionalCost += 4;
		
//		if (hasExpensiveType)
//			additionalCost += 1;

		return 0 + additionalCost + (group.getFreeVarCount()/group.getStatements().size());
		
	}
	
	/**
	 * If the subject is not the same for all triple patterns in
	 * the group, an additional cost of .5 is considered.
	 * 
	 * Example:
	 * ?x p o . ?x p2 o2 => cost is 0
	 * ?x p ?s . ?s ?p2 val => additional cost is 0.5
	 * 
	 * @return
	 */
	private static double computeAdditionPatternCost(List<ExclusiveStatement> stmts) {
		
		String s = null;
		for (ExclusiveStatement st : stmts) {
			String currentVar = QueryStringUtil.toString(st.getSubjectVar());
			if (!currentVar.equals(s) && s!=null)
				return 0.5;
			s = currentVar;				
		}
		return 0.0;
	}
	

	protected static double estimateCost(ExclusiveStatement owned, Set<String> joinVars) {
		
		/* currently the cost is the number of free vars that are executed in the join */
		
		int count = 100;		
		
		// special heuristic: if exclusive statement with one free variable, more selective than owned group
		// with more than 3 free variable
		if (owned.getFreeVarCount()<=1 && joinVars.size()==0) 
			count = 3;
		
		for (String var : owned.getFreeVars())
			if (!joinVars.contains(var))
				count++;
		
		return count;
	}
	
	protected static double estimateCost(FedXService service, Set<String> joinVars) {
		
		int additionalCost = 0;
		
		// evaluate services with variable service ref late (since the service ref 
		// may be computed at evaluation time)
		if (!service.getService().getServiceRef().hasValue()) {
			additionalCost += 1000;
		}
		
		if (service.getNumberOfTriplePatterns()<=1) {
			if (joinVars.size()==0 && service.getFreeVarCount()<=1)
				additionalCost = 3;	// compare exclusive statement
			else
				additionalCost += 100;	// compare all statements
		}

		return 0 + additionalCost + service.getFreeVarCount();
	}
	
	protected static double estimateCost(StatementSourcePattern stmt, Set<String> joinVars) {
		
		/* currently the cost is the number of free vars that are executed in the join */
		
		int count = 100;
		for (String var : stmt.getFreeVars())
			if (!joinVars.contains(var))
				count++;
		
		return count;
	}

	

	protected static double estimateCost(NUnion nunion, Set<String> joinVars) {
		
		// the unions cost is determined is determined by the minimum cost 
		// of the children + penalty cost of number of arguments
		double min = Double.MAX_VALUE;
		for (TupleExpr t : nunion.getArgs() ) {
			double cost = estimateCost(t, joinVars);
			if (cost < min)
				min = cost;
		}
		
		return min + nunion.getNumberOfArguments() - 1;
	}
	
	protected static double estimateCost(NJoin join, Set<String> joinVars) {
		
		// cost of a join is determined by the cost of the first join arg
		// Note: the join order of this join is already determined (depth first)
		// in addition we add a penalty for the number of join arguments
		double cost = estimateCost(join.getArg(0), joinVars);
		
		return cost + join.getNumberOfArguments() - 1;
	}
}
