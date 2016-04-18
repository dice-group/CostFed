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

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.fluidops.fedx.FederationManager;
import com.fluidops.fedx.exception.FedXRuntimeException;
import com.fluidops.fedx.monitoring.MonitoringImpl.MonitoringInformation;
import com.fluidops.fedx.monitoring.jmx.FederationStatus;


public class MonitoringUtil
{

	public static void printMonitoringInformation() {
		
		MonitoringService ms = getMonitoringService();

		System.out.println("### Request monitoring: ");
		for (MonitoringInformation m : ms.getAllMonitoringInformation()) {
			System.out.println("\t" + m.toString());
		}
	}
	
	
	public static MonitoringService getMonitoringService() throws FedXRuntimeException {
		Monitoring m = FederationManager.getMonitoringService();
		if (m instanceof MonitoringService)
			return (MonitoringService)m;
		throw new FedXRuntimeException("Monitoring is currently disabled for this system.");
	}
	
	
	/**
	 * Flag is set to true to once initialized
	 */
	private static boolean JMX_initialized = false;
	
	/**
	 * Initialize JMX monitoring using the systems MBeanServer. JMX
	 * is only initialized if it has not been initialized before.
	 * 
	 * @throws Exception
	 */
	public static void initializeJMXMonitoring() throws Exception
	{
		if (JMX_initialized)
			return;
		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		ObjectName monitoring = new ObjectName("com.fluidops.fedx:type=FederationStatus");
		mbs.registerMBean(new FederationStatus(), monitoring);
		JMX_initialized = true;
	}
}
