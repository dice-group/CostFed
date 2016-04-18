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
import java.util.List;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Value;
import org.openrdf.query.Binding;
import org.openrdf.query.algebra.And;
import org.openrdf.query.algebra.Compare;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Not;
import org.openrdf.query.algebra.Or;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.Service;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.AbstractQueryModelVisitor;
import org.openrdf.query.impl.SimpleBinding;

import com.fluidops.fedx.algebra.EmptyResult;
import com.fluidops.fedx.algebra.FilterExpr;
import com.fluidops.fedx.algebra.FilterTuple;
import com.fluidops.fedx.algebra.ProjectionWithBindings;
import com.fluidops.fedx.algebra.StatementTupleExpr;
import com.fluidops.fedx.exception.OptimizationException;

/**
 * Filter optimizer to push down FILTER expressions as far as possible.
 * 
 * @author Andreas Schwarte
 *
 */
public class FilterOptimizer extends AbstractQueryModelVisitor<OptimizationException> implements FedXOptimizer {

	public static Logger log = Logger.getLogger(FilterOptimizer.class);
	
	/* map containing the inserted values, i.e. filter values which could be
	 * directly replaced into the query
	 */
	private List<Binding> insertedValues = new ArrayList<Binding>();
	
	@Override
	public void optimize(TupleExpr tupleExpr) {
		tupleExpr.visit(this);				
	}

	
	@Override
	public void meet(Filter filter)  {
		
		if (filter.getArg() instanceof EmptyResult) {
			log.debug("Argument of filter expression does not yield results at the provided sources, replacing Filter node.");
			filter.replaceWith(filter.getArg());
			return;
		}
					
		/*
		 * TODO idea:
		 * if we have a FILTER such as ?s='a' OR ?s='b' OR ?s='c' handle this appropriately
		 */
		
		ValueExpr valueExpr = filter.getCondition();
		
		/*
		 * TODO transform condition into some normal form, e.g. CNF
		 */
		
		// determine conjunctive expressions
		List<ValueExpr> conjunctiveExpressions = new ArrayList<ValueExpr>();
		getConjunctiveExpressions(valueExpr, conjunctiveExpressions);
				
		FilterExprInsertVisitor filterExprVst = new FilterExprInsertVisitor();
		List<ValueExpr> remainingExpr = new ArrayList<ValueExpr>(conjunctiveExpressions.size());
		
		for (ValueExpr cond : conjunctiveExpressions) {
			
			/*
			 * Determine if this filter is applicable for optimization.
			 * Currently only leaf expressions are applicable, i.e.
			 * not combined expressions.
			 */
			if (isCompatibleExpr(cond)) {
							
				HashSet<String> exprVars = new VarFinder().findVars(cond);
				FilterExpr filterExpr = new FilterExpr(cond, exprVars);
				
				filterExprVst.initialize(filterExpr);
				filter.getArg().visit(filterExprVst);
				
				// if the filter expr. is handled in the stmt we do not have to keep it
				if (filterExprVst.canRemove())
					continue;
				
				remainingExpr.add(filterExpr.getExpression());
				
			} else {
				remainingExpr.add(cond);
			}
			
		}
		
		if (remainingExpr.size()==0) {
			filter.replaceWith(filter.getArg()); 	// remove the filter			
		}
		
		else if (remainingExpr.size()==1) {
			filter.setCondition(remainingExpr.get(0));		// just apply the remaining condition
		}
		
		else {
			
			// construct conjunctive value expr
			And root = new And();	
			root.setLeftArg(remainingExpr.get(0));
			And tmp = root;
			for (int i=1; i<remainingExpr.size()-1; i++) {
				And _a = new And();
				_a.setLeftArg(remainingExpr.get(i));
				tmp.setRightArg(_a);
				tmp = _a;				
			}
			tmp.setRightArg(remainingExpr.get(remainingExpr.size()-1));
			
			filter.setCondition(root);
		}
		
	}
		
	
	
	@Override
	public void meet(Projection node) throws OptimizationException {
		super.meet(node);
		// we need to check if we have inserted some value constant
		// from a filter and if this is part of the projection. If yes
		// we need to actually project it
		if (!insertedValues.isEmpty()) {
			node.replaceWith(new ProjectionWithBindings(node.getArg(), node.getProjectionElemList(), insertedValues));
		}
	}


	@Override
	public void meet(Service node) throws OptimizationException	{
		// do not optimize anything within SERVICE
	}


	/**
	 * add the conjunctive expressions to specified list, has recursive step.
	 *
	 * @param expr
	 * 			the expr, in the best case in CNF
	 * @param conjExpr
	 * 			the list to which expressions will be added
	 */
	protected void getConjunctiveExpressions(ValueExpr expr, List<ValueExpr> conjExpr) {
		if (expr instanceof And) {
			And and = (And)expr;
			getConjunctiveExpressions(and.getLeftArg(), conjExpr);
			getConjunctiveExpressions(and.getRightArg(), conjExpr);
		} else
			conjExpr.add(expr);
	}
	
	
	/**
	 * returns true if this filter can be used for optimization. Currently no
	 * conjunctive or disjunctive expressions are supported.
	 * 
	 * @param e
	 * @return
	 */
	protected boolean isCompatibleExpr(ValueExpr e) {
		
		if (e instanceof And || e instanceof Or) {
			return false;
		}
		
		if (e instanceof Not) {
			return isCompatibleExpr( ((Not)e).getArg() );
		}
		
		return true;
	}
	
	
	
	protected class VarFinder extends AbstractQueryModelVisitor<OptimizationException> {
		
		protected HashSet<String> vars;
		
		public HashSet<String> findVars(ValueExpr expr) {
			vars = new HashSet<String>();
			expr.visit(this);			
			return vars;
		}
		
		
		@Override
		public void meet(Var var) {
			if (var.getValue()==null)
				vars.add(var.getName());
			super.meet(var);
		}
	}
	
	
	protected class FilterExprInsertVisitor extends AbstractQueryModelVisitor<OptimizationException> {
		
		
		protected boolean canRemove = false;		// if true, the current filter can be removed
		protected FilterExpr filterExpr = null;		// the current filter Expr
		
		
		
		public void initialize(FilterExpr filterExpr) {
			this.canRemove=true;
			this.filterExpr = filterExpr;
		}
		
		public boolean canRemove() {
			return canRemove;
		}
		
		
		@Override
		public void meetOther(QueryModelNode node) {
			
			if (node instanceof FilterTuple) {
				handleFilter((FilterTuple)node, filterExpr);
			}
			
			else if (node instanceof StatementTupleExpr) {
			
				// TODO check if we still can remove!!!
				
			}
			
			else {
				// TODO we maybe have to adjust canRemove here
				super.meetOther(node);
			}
		}
		
		
		private void handleFilter(FilterTuple filterTuple, FilterExpr expr) {
			
			/*
			 * CompareEQ expressions are inserted as bindings if possible
			 * 
			 * if the filtertuple contains all vars of the filterexpr, we
			 * can evaluate the filter expr safely on the filterTuple
			 * 
			 * if there is no intersection of variables, the filter is 
			 * irrelevant for this expr
			 * 
			 * if there is some intersection, we cannot remove the filter
			 * and have to keep it in the query plan for postfiltering
			 */
			int intersected = 0;
			for (String filterVar : expr.getVars()) {
				if (filterTuple.getFreeVars().contains(filterVar))
					intersected++;
			}
			
			// filter expression is irrelevant
			if (intersected==0)
				return;
			
			// push eq comparison into stmt as bindings
			if (expr.isCompareEq()) {
				
				if (handleCompare(filterTuple, (Compare)expr.getExpression()))
					return;
			}
			
			// filter contains all variables => push filter
			if (intersected==expr.getVars().size())
				filterTuple.addFilterExpr(expr);
			
			// filter is still needed for post filtering
			else {
				canRemove=false;
			}
		}
		
		
		
		private boolean handleCompare(FilterTuple filterTuple, Compare cmp) {
			
			boolean isVarLeft = cmp.getLeftArg() instanceof Var;
			boolean isVarRight = cmp.getRightArg() instanceof Var;
			
			// cases
			// 1. both vars: we cannot add binding
			// 2. left var, right value -> add binding
			// 3. right var, left value -> add binding
			//
			// Note: we restrict this optimization to values of type Resource
			// since for instance subj can only be URIs (i.e. literals are 
			// not allowed). For other types the Filter remains in place.
			
			if (isVarLeft && isVarRight)
				return false;
			
			if (isVarLeft && cmp.getRightArg() instanceof ValueConstant) {
				String varName = ((Var)cmp.getLeftArg()).getName();
				Value value = ((ValueConstant)cmp.getRightArg()).getValue();
				if (value instanceof Resource) {
					filterTuple.addBoundFilter(varName, value);
					insertedValues.add(new SimpleBinding(varName, value));
					return true;
				}
			}
			
			if (isVarRight && cmp.getLeftArg() instanceof ValueConstant) {
				String varName = ((Var)cmp.getRightArg()).getName();
				Value value = ((ValueConstant)cmp.getLeftArg()).getValue();
				if (value instanceof Resource) {
					filterTuple.addBoundFilter(varName, value);
					insertedValues.add(new SimpleBinding(varName, value));
					return true;
				}
			}
			
			return false;	// not added
		}
	}
}
