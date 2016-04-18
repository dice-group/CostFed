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

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * Interface for any scheduler. 
 * 
 * @author Andreas Schwarte
 *
 * @see ControlledWorkerScheduler
 */
public interface Scheduler {
	/**
	 * Schedule the provided tasks.
	 * 
	 * @param task
	 */
	void schedule(Task task);
	
	<R> Future<R> schedule(Callable<R> task, int priority);
	
	/**
	 * Shutdown all threads.
	 */
	void shutdown();

	/**
	 * Determine if the scheduler has unfisnihed tasks.
	 * 
	 * @return
	 */
	boolean isRunning();
}
