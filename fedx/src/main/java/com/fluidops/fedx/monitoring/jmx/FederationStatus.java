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

package com.fluidops.fedx.monitoring.jmx;

import java.util.ArrayList;
import java.util.List;

import org.openrdf.query.BindingSet;

import com.fluidops.fedx.FederationManager;
import com.fluidops.fedx.evaluation.concurrent.ControlledWorkerScheduler;
import com.fluidops.fedx.structures.Endpoint;

public class FederationStatus implements FederationStatusMBean {

	@Override
	public List<String> getFederationMembersDescription() {
		List<Endpoint> members = FederationManager.getInstance().getFederation().getMembers();
		List<String> res = new ArrayList<String>();		
		for (Endpoint e : members) 
			res.add(e.toString());
		return res;
	}

	@Override
	public int getIdleJoinWorkerThreads() {
		ControlledWorkerScheduler scheduler = FederationManager.getInstance().getScheduler();
		return 0;
		//return scheduler.getNumberOfIdleWorkers();
	}

	@Override
	public int getTotalJoinWorkerThreads() {
		ControlledWorkerScheduler scheduler = FederationManager.getInstance().getScheduler();
		return 0;
		//return scheduler.getTotalNumberOfWorkers();
	}

	@Override
	public int getIdleUnionWorkerThreads() {
		ControlledWorkerScheduler scheduler = FederationManager.getInstance().getScheduler();
		return 0;
		//return scheduler.getNumberOfIdleWorkers();
	}

	@Override
	public int getTotalUnionWorkerThreads() {
		ControlledWorkerScheduler scheduler = FederationManager.getInstance().getScheduler();
		return 0;
		//return scheduler.getTotalNumberOfWorkers();
	}

	@Override
	public int getNumberOfScheduledJoinTasks() {
		ControlledWorkerScheduler scheduler = FederationManager.getInstance().getScheduler();
		return 0;
		//return scheduler.getNumberOfTasks();
	}

	@Override
	public int getNumberOfScheduledUnionTasks() {
		ControlledWorkerScheduler scheduler = FederationManager.getInstance().getScheduler();
		return 0;
		//return scheduler.getNumberOfTasks();
	}
}
