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

package com.fluidops.fedx.evaluation.union;

import com.fluidops.fedx.structures.QueryInfo;

/**
 * Base class for worker unions providing convenience functions to add tasks.
 * 
 * @author Andreas Schwarte
 * 
 * @see SynchronousWorkerUnion
 * @see ControlledWorkerUnion
 */
public abstract class WorkerUnionBase<T> extends UnionExecutorBase<T> {

	//protected List<Runnable> tasks = new ArrayList<Runnable>();
	protected QueryInfo queryInfo = null;
	
	public WorkerUnionBase(QueryInfo queryInfo) {
		super();
		this.queryInfo = queryInfo;
	}
	

//	/**
//	 * Add a generic parallel task. Note that it is required that the task has 
//	 * this instance as its control.
//	 * 
//	 * @param task
//	 */
//	public void addTask(Runnable task) {
//		tasks.add( task);
//	}
	
	//@Override
	public int getQueryId() {
		if (queryInfo != null)
			return queryInfo.getQueryID();
		return -1;
	}
}
