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

package com.fluidops.fedx.evaluation.iterator;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.LookAheadIteration;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.openrdf.model.Value;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

/**
 * Inserts original bindings into the result.
 * 
 * @author Andreas Schwarte
 */
public class IndependentJoingroupBindingsIteration3 extends LookAheadIteration<BindingSet, QueryEvaluationException>{

	// a pattern matcher for the binding resolver, pattern: myVar_%outerID%#bindingId, e.g. name_0#0
	protected static final Pattern pattern = Pattern.compile("(.*)_(.*)_(.*)");	
	
	protected final List<BindingSet> bindings;
	protected final CloseableIteration<BindingSet, QueryEvaluationException> iter;
	protected ArrayList<BindingSet> result = null;
	protected int currentIdx = 0;
	
	public IndependentJoingroupBindingsIteration3(CloseableIteration<BindingSet, QueryEvaluationException> iter, List<BindingSet> bindings) {
		this.bindings = bindings;
		this.iter = iter;
	}

	@Override
	protected BindingSet getNextElement() throws QueryEvaluationException {
		
		if (result==null) {
			result = computeResult();
		}
		
		if (currentIdx>=result.size())
			return null;
		
		return result.get(currentIdx++);
	}

	
	protected ArrayList<BindingSet> computeResult() throws QueryEvaluationException {
		
		// underlying arraylist serves as map, index corresponds to bindings index (i.e. at most bindings.size() - 1)
		// a_res[0] = { v_0#0-1; v_0#0-2; ... }
		// a_res[1] = { v_0#1-1; v_0#1-2; ... }
		ArrayList<LinkedList<BindingInfo>> a_res = new ArrayList<LinkedList<BindingInfo>>(bindings.size());
		ArrayList<LinkedList<BindingInfo>> b_res = new ArrayList<LinkedList<BindingInfo>>(bindings.size());
		
		// we assume that each binding returns at least one result for each statement
		// => create lists in advance to avoid checking later on
		for (int i=0; i<bindings.size(); i++) {
			a_res.add(new LinkedList<BindingInfo>());
			b_res.add(new LinkedList<BindingInfo>());
		}
		
		// assumes that bindingset of iteration has exactly one binding
		while (iter.hasNext()) {
			
			BindingSet bIn = iter.next();
			
			if (bIn.size()!=1)
				throw new RuntimeException("For this optimization a bindingset needs to have exactly one binding, it has " + bIn.size() + ": " + bIn);

			Binding b = bIn.getBinding( bIn.getBindingNames().iterator().next() );
			
			// name is something like myVar_%outerID%_bindingId, e.g. name_0_0
			Matcher m = pattern.matcher(b.getName());
			if (!m.find())
				throw new QueryEvaluationException("Unexpected pattern for binding name: " + b.getName());
			
			BindingInfo bInfo = new BindingInfo(m.group(1), Integer.parseInt(m.group(3)), b.getValue());
			int bIndex = Integer.parseInt(m.group(2));
			
//			int tmp = b.getName().indexOf("_");
//			String pattern = b.getName().substring(tmp+1);
//			String split[] = pattern.split("_");
//			
//			int bIndex = Integer.parseInt(split[0]);
//			int bindingsIdx = Integer.parseInt(split[1]);
//			BindingInfo bInfo = new BindingInfo(b.getName().substring(0, tmp), bindingsIdx, b.getValue());
			
			// add a new binding info to the correct result list
			if (bIndex==0) {
				a_res.get(bInfo.bindingsIdx).add(bInfo);
			}
			else if (bIndex==1)
				b_res.get(bInfo.bindingsIdx).add(bInfo);
			else
				throw new RuntimeException("Unexpected binding value.");
		}
		
		// TODO think about a better upper bound or use linked list
		ArrayList<BindingSet> res = new ArrayList<BindingSet>(2*bindings.size());
		
		for (int a_idx=0; a_idx<a_res.size(); a_idx++) {
			LinkedList<BindingInfo> a_list = a_res.get(a_idx);			
			for (BindingInfo a : a_list) {
				for (BindingInfo b : b_res.get(a_idx)) {
					QueryBindingSet newB = new QueryBindingSet(bindings.size() + 2);
					newB.addBinding(a.name, a.value);
					newB.addBinding(b.name, b.value);
					newB.addAll(bindings.get(a.bindingsIdx));
					res.add(newB);
				}
			}
		}
				
		return res;
	}
	
	
	protected class BindingInfo {
		public final String name;
		public final int bindingsIdx;
		public final Value value;
		public BindingInfo(String name, int bindingsIdx, Value value) {
			super();
			this.name = name;
			this.bindingsIdx = bindingsIdx;
			this.value = value;
		}	
		public String toString() {
			return name + ":" + value.stringValue();
		}
	}

}
