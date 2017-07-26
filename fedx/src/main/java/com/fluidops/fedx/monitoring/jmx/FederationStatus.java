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

import java.util.List;

import com.fluidops.fedx.FedX;
import com.fluidops.fedx.evaluation.concurrent.ControlledWorkerScheduler;

public class FederationStatus implements FederationStatusMBean {

    final FedX federation;
    public FederationStatus(FedX federation) {
        this.federation = federation;
    }
    
	@Override
	public List<String> getFederationMembersDescription() {
	    throw new RuntimeException("not implemented");
//		List<Endpoint> members = FederationManager.getInstance().getFederation().getMembers();
//		List<String> res = new ArrayList<String>();		
//		for (Endpoint e : members) 
//			res.add(e.toString());
//		return res;
	}

	@Override
	public int getIdleJoinWorkerThreads() {
		ControlledWorkerScheduler scheduler = federation.getScheduler();
		return 0;
		//return scheduler.getNumberOfIdleWorkers();
	}

	@Override
	public int getTotalJoinWorkerThreads() {
		ControlledWorkerScheduler scheduler = federation.getScheduler();
		return 0;
		//return scheduler.getTotalNumberOfWorkers();
	}

	@Override
	public int getIdleUnionWorkerThreads() {
		ControlledWorkerScheduler scheduler = federation.getScheduler();
		return 0;
		//return scheduler.getNumberOfIdleWorkers();
	}

	@Override
	public int getTotalUnionWorkerThreads() {
		ControlledWorkerScheduler scheduler = federation.getScheduler();
		return 0;
		//return scheduler.getTotalNumberOfWorkers();
	}

	@Override
	public int getNumberOfScheduledJoinTasks() {
		ControlledWorkerScheduler scheduler = federation.getScheduler();
		return 0;
		//return scheduler.getNumberOfTasks();
	}

	@Override
	public int getNumberOfScheduledUnionTasks() {
		ControlledWorkerScheduler scheduler = federation.getScheduler();
		return 0;
		//return scheduler.getNumberOfTasks();
	}
}
