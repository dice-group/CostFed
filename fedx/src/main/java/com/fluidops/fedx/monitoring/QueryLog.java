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

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

import com.fluidops.fedx.structures.QueryInfo;

/**
 * Convenience class which writes the query backlog to a file, 
 * default: logs/queryLog.log
 * 
 * @author Andreas Schwarte
 *
 */
public class QueryLog
{
	public static Logger log = Logger.getLogger(QueryLog.class);
	
	private Logger queryLog;
	private File queryLogFile = new File("logs", "queryLog.log");

	public QueryLog() throws IOException {
		log.info("Initializing query log, output file: " + queryLogFile.getName());
		initQueryLog();
	}
	
	private void initQueryLog() throws IOException {
		queryLog = Logger.getLogger("QueryBackLog");
		queryLog.setAdditivity(false);
		queryLog.setLevel(Level.INFO);
		queryLog.removeAllAppenders();
		
		Layout layout = new PatternLayout("%d{yyyy-MM-dd HH:mm:ss}: %m%n");
		
		RollingFileAppender appender = new RollingFileAppender(layout, queryLogFile.getAbsolutePath(), true);
		appender.setMaxFileSize("1024KB");
		appender.setMaxBackupIndex(5);
		queryLog.addAppender(appender);		
	}
	
	public void logQuery(QueryInfo query) {
		queryLog.info(query.getQuery().replace("\r\n", " ").replace("\n", " "));
		if (log.isTraceEnabled())
			log.trace("#Query: " + query.getQuery());
	}

}
