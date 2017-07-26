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

package com.fluidops.fedx.statistics;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.algebra.StatementPattern;

import com.fluidops.fedx.algebra.ExclusiveGroup;
import com.fluidops.fedx.algebra.StatementSource;

public class StatisticsImpl implements Statistics {

	@Override
	public int estimatedResults(Statement stmt, StatementSource source) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean hasResults(Statement stmt, StatementSource source) {
		return false;
	}

	@Override
	public double selectivity(StatementPattern stmt) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double selectivity(ExclusiveGroup group) {
		// TODO Auto-generated method stub
		return 0;
	}

}
