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

import org.eclipse.rdf4j.query.algebra.TupleExpr;

import com.fluidops.fedx.Config;


/**
 * Monitoring facility to maintain the query execution plan in
 * a variable local to the executing thread. Can be used to 
 * retrieve the query plan from the outside in the evaluation
 * thread.
 * 
 * This module is only active if {@link Config#isLogQueryPlan()}
 * is enabled. In addition {@link Config#isEnableMonitoring()} must
 * be set. In any other case, this class is a void operation.
 * 
 * @author Andreas Schwarte
 *
 */
public class QueryPlanLog
{

	static ThreadLocal<String> queryPlan = new ThreadLocal<String>();
	
	Config config;
	public QueryPlanLog(Config config) {
	    this.config = config;
	}
	
	public String getQueryPlan() {
		if (!isActive() || !config.isEnableMonitoring())
			throw new IllegalStateException("QueryPlan log module is not active, use monitoring.logQueryPlan=true in the configuration to activate.");
		return queryPlan.get();
	}
	
	public void setQueryPlan(TupleExpr query) {
		if (!isActive())
			return;
		queryPlan.set(query.toString());
	}
	
	private boolean isActive() {
		return config.isLogQueryPlan();
	}
	
}
