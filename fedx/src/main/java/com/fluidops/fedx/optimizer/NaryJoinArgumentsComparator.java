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

import java.util.Comparator;

import org.openrdf.query.algebra.TupleExpr;

import com.fluidops.fedx.algebra.ExclusiveStatement;
import com.fluidops.fedx.algebra.ExclusiveGroup;


/**
 * Comparator:
 * 
 * partial order: OwnedStatementSourcePatternGroup -> OwnedStatementSourcePattern -> StatementSourcePattern
 * 
 * @author Andreas
 *
 */
public class NaryJoinArgumentsComparator implements Comparator<TupleExpr>{

	
	@Override
	public int compare(TupleExpr a, TupleExpr b) {

		if (a instanceof ExclusiveGroup) {
			if (b instanceof ExclusiveGroup)
				return 0;
			else
				return -1;
		}
		
		else if (b instanceof ExclusiveGroup) {
			return 1;
		}
		
		else if (a instanceof ExclusiveStatement) {
			if (b instanceof ExclusiveStatement)
				return 0;		// 0
			else
				return -1;		// -1
		}
		
		else if (b instanceof ExclusiveStatement) {
			return 1;			// 1
		}
			
		// XXX compare number of free variables
		
		return 0;
	}

}
