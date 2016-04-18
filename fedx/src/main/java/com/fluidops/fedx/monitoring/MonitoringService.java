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

package com.fluidops.fedx.monitoring;

import java.util.List;

import com.fluidops.fedx.monitoring.MonitoringImpl.MonitoringInformation;
import com.fluidops.fedx.structures.Endpoint;

public interface MonitoringService extends Monitoring
{
	
	public MonitoringInformation getMonitoringInformation(Endpoint e);
	
	public List<MonitoringInformation> getAllMonitoringInformation();

}
