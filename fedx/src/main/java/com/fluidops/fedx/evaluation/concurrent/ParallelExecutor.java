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

package com.fluidops.fedx.evaluation.concurrent;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;

import org.eclipse.rdf4j.query.QueryEvaluationException;

import com.fluidops.fedx.evaluation.join.JoinExecutorBase;
import com.fluidops.fedx.evaluation.union.UnionExecutorBase;


/**
 * Interface for any parallel cursor, i.e. result iterations. Implementations can act 
 * as control for scheduler implementations, e.g. {@link ControlledWorkerScheduler}. 
 * The common use case is to pass results from the scheduler to the controlling
 * result iteration.
 * 
 * @author Andreas Schwarte
 * 
 * @see JoinExecutorBase
 * @see UnionExecutorBase
 */
@Deprecated
public interface ParallelExecutor<T> extends Runnable {

	/**
	 * Handle the result appropriately, e.g. add it to the result iteration. Take care
	 * for synchronization in a multithreaded environment
	 * 
	 * @param res
	 */
	public void addResult(CloseableIteration<T, QueryEvaluationException> res);
	
	/**
	 * Toss some exception to the controlling instance
	 * 
	 * @param e
	 */
	public void toss(Exception e);
	
	/**
	 * Inform the controlling instance that some job is done from a different thread. In most cases this is a no-op.
	 */
	public void done();
	
	
	/**
	 * Return true if this executor is finished or aborted
	 * 
	 * @return
	 */
	public boolean isFinished();

	
	/**
	 * Return the query id of the associated query, or -1 if unknown
	 * 
	 * @return
	 */
	public int getQueryId();
}
