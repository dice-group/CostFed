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

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;

import com.fluidops.fedx.structures.UnboundStatement;

public class StatisticsUtil {

	
	public static Statement toStatement(StatementPattern stmt, BindingSet bindings) {
		
		Value subj = toValue(stmt.getSubjectVar(), bindings);
		Value pred = toValue(stmt.getPredicateVar(), bindings);
		Value obj = toValue(stmt.getObjectVar(), bindings);
		
		return new UnboundStatement((Resource)subj, (IRI)pred, obj);
	}
	
	protected static Value toValue(Var var, BindingSet bindings) {
		if (var.hasValue())
			return var.getValue();
		
		if (bindings.hasBinding(var.getName()))	
			return bindings.getValue(var.getName());
		
		return null;			
	}
}
