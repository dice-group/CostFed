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

package com.fluidops.fedx.structures;

import java.io.Serializable;

import org.openrdf.model.Resource;
import org.openrdf.model.IRI;
import org.openrdf.model.Value;
import org.openrdf.query.algebra.StatementPattern;

public class SubQuery implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8968907794785828994L;
	
	protected String subj = null;
	protected String pred = null;
	protected String obj = null;
	
	public SubQuery(String subj, String pred, String obj) {
		super();
		this.subj = subj;
		this.pred = pred;
		this.obj = obj;
	}
	
	public SubQuery(Resource subj, IRI pred, Value obj) {
		super();
		if (subj!=null)
			this.subj = subj.stringValue();
		if (pred!=null)
			this.pred = pred.stringValue();
		if (obj!=null)
			this.obj = obj.toString();	
		// we need to take toString() here since stringValue for literals does not contain the datatype
	}

	public SubQuery(StatementPattern stmt) {
		super();
		
		if (stmt.getSubjectVar().hasValue())
			subj = stmt.getSubjectVar().getValue().stringValue();
		if (stmt.getPredicateVar().hasValue())
			pred = stmt.getPredicateVar().getValue().stringValue();
		if (stmt.getObjectVar().hasValue())
			obj = stmt.getObjectVar().getValue().stringValue();
	}	
	
	@Override
	public int hashCode() {
		final int prime1 = 961;
		final int prime2 = 31;
		final int prime3 = 1;
		int result = 1;
		result += ((subj == null) ? 0 : subj.hashCode() * prime1);
		result += ((pred == null) ? 0 : pred.hashCode() * prime2);
		result += ((obj == null) ? 0 : obj.hashCode() * prime3);		
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SubQuery other = (SubQuery) obj;
		if (this.obj == null) {
			if (other.obj != null)
				return false;
		} else if (!this.obj.equals(other.obj))
			return false;
		if (pred == null) {
			if (other.pred != null)
				return false;
		} else if (!pred.equals(other.pred))
			return false;
		if (subj == null) {
			if (other.subj != null)
				return false;
		} else if (!subj.equals(other.subj))
			return false;
		return true;
	}
	
	
}
