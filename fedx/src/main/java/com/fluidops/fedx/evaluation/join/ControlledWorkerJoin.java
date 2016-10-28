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

package com.fluidops.fedx.evaluation.join;

import org.apache.log4j.Logger;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;

import com.fluidops.fedx.evaluation.FederationEvalStrategy;
import com.fluidops.fedx.evaluation.concurrent.ControlledWorkerScheduler;
import com.fluidops.fedx.structures.QueryInfo;

import info.aduna.iteration.CloseableIteration;


/**
 * Execute the nested loop join in an asynchronous fashion, i.e. one binding after the other (but
 * concurrently)
 *  
 * The number of concurrent threads is controlled by a {@link ControlledWorkerScheduler} which
 * works according to the FIFO principle.
 * 
 * This join cursor blocks until all scheduled tasks are finished, however the result iteration
 * can be accessed from different threads to allow for pipelining.
 * 
 * @author Andreas Schwarte
 */
public class ControlledWorkerJoin extends JoinExecutorBase<BindingSet> {

	private static Logger log = Logger.getLogger(ControlledWorkerJoin.class);
	
	private final ControlledWorkerScheduler scheduler;
	
	public ControlledWorkerJoin(ControlledWorkerScheduler scheduler, FederationEvalStrategy strategy,
			CloseableIteration<BindingSet, QueryEvaluationException> leftIter,
			TupleExpr rightArg, BindingSet bindings, QueryInfo queryInfo)
	{
		super(strategy, leftIter, rightArg, bindings, queryInfo);
		this.scheduler = scheduler;
		handleBindings();
	}

	@Override
	protected void doAddTask(JoinExecutorBase<BindingSet>.JoinTask jt) {
		scheduler.schedule(jt);
	}
	
	@Override
	protected void handleBindings() {
		
		int totalBindings = 0;		// the total number of bindings
		
		while (!closed && leftIter.hasNext()) {
			ParallelJoinTask task = new ParallelJoinTask(strategy, rightArg, leftIter.next());
			totalBindings++;
			addTask(task);
		}
		
		// XXX remove output if not needed anymore
		log.debug("JoinStats: left iter of join #" + this.joinId + " had " + totalBindings + " results.");
		
//		// wait until all tasks are executed
//		synchronized (this) {
//			try {
//				// check to avoid deadlock
//				while (scheduler.isRunning(this)) {
//					this.wait();
//				}
//			} catch (InterruptedException e) {
//				;	// no-op
//			}
//		}

	}

	
	// TODO the abort is handled differently now: see FederationManager
//	@Override
//	public void toss(Exception e) {
//		int queryId=getQueryId();
//		if (queryId<0)
//			scheduler.abort(this);		// abort all tasks belonging to this join
//		else 
//			scheduler.abort(queryId);
//		synchronized (this) {
//			this.finished=true;
//		}
//		super.toss(e);
//	}

}
