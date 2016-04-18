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

import com.fluidops.fedx.evaluation.concurrent.ControlledWorkerScheduler;
import com.fluidops.fedx.structures.QueryInfo;

/**
 * Execution of union tasks with {@link ControlledWorkerScheduler}. Tasks can be added
 * using the provided functions. Note that the union operation is to be executed
 * with the {@link #run()} method (also threaded execution is possible). Results are
 * then contained in this iteration.
 *
 * @author Andreas Schwarte
 *
 */
public class ControlledWorkerUnion<T> extends WorkerUnionBase<T> {

	public static int waitingCount = 0;
	public static int awakeCount = 0;
	
	protected final ControlledWorkerScheduler scheduler;
	
	public ControlledWorkerUnion(ControlledWorkerScheduler scheduler, QueryInfo queryInfo) {
		super(queryInfo);
		this.scheduler = scheduler;
	}

	@Override
	protected void doAddTask(UnionExecutorBase<T>.UnionTask ut) {
		scheduler.schedule(ut);
	}
}
