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

import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;

import info.aduna.iteration.CloseableIteration;


/**
 * Interface for any expression that can be evaluated
 * 
 * @author Andreas Schwarte
 *
 * @see StatementSourcePattern
 * @see ExclusiveStatement
 * @see ExclusiveGroup
 */
public interface StatementTupleExpr extends TupleExpr, FedXExpr, QueryRef {

	/**
	 * @return
	 * 		the id of this expr
	 */
	public int getId();
	
	/**
	 * @return
	 * 		a list of free (i.e. unbound) variables in this expression
	 */
	public List<String> getFreeVars();
	
	/**
	 * @return
	 * 		the number of free (i.e. unbound) variables in this expression
	 */
	public int getFreeVarCount();
	
	/**
	 * @return
	 * 		a list of sources that are relevant for evaluation of this expression
	 */
	public List<StatementSource> getStatementSources();
	
	/**
	 * returns true iff this statement has free variables in the presence
	 * of the specified binding set
	 * 
	 * @param binding
	 * @return
	 */
	public boolean hasFreeVarsFor(BindingSet binding);
	
	/**
	 * Evaluate this expression using the provided bindings
	 * 
	 * @param bindings
	 * @return
	 * 			the result iteration
	 * 
	 * @throws QueryEvaluationException
	 */
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(BindingSet bindings); 
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(List<BindingSet> bindings);
	
	/**
	 * Sets/adds a variable names that is local to this expression
	 * 
	 * @param localVars
	 */
	public void addLocalVar(String localVars);
	
	/**
	 * Retrieve the variable names that are local to this expression,
	 * i.e. the ones that do not need to be projected.
	 * 
	 */
	public List<String> getLocalVars();
}
