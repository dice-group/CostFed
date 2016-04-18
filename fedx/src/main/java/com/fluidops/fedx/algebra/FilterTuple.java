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

import org.openrdf.model.Value;

/**
 * Expressions implementing this interface can apply some {@link FilterValueExpr}
 * during evaluation.
 * 
 * @author Andreas Schwarte
 * 
 * @see StatementSourcePattern
 * @see ExclusiveStatement
 * @see ExclusiveGroup
 * 
 */
public interface FilterTuple {

	/**
	 * @return
	 * 			true if this expression has a filter to apply
	 */
	public boolean hasFilter();
	
	/**
	 * register a new filter expression. If the expr has already a filter registered, the
	 * new expression is added to a {@link ConjunctiveFilterExpr}.
	 * 
	 * @param expr
	 */
	public void addFilterExpr(FilterExpr expr);
	
	
	/**
	 * register a filter that can be directly expressed as a binding, e.g.
	 * 
	 * SELECT * WHERE {
	 *  ?s p o .
	 *  FILTER (?s = X)
	 * }
	 * 
	 * is equivalent to 
	 * 
	 * SELECT * WHERE {
	 * 	X p o .
	 * }
	 * 
	 * @param varName
	 * @param value
	 */
	public void addBoundFilter(String varName, Value value);
	
	
	/**
	 * 
	 * @return
	 * 		the currently registered filter expressions, usually of type {@link FilterExpr}
	 * 		or {@link ConjunctiveFilterExpr}
	 */
	public FilterValueExpr getFilterExpr();
	
	/**
	 * @return
	 * 			the free variables of this expression
	 */
	public List<String> getFreeVars();
}
