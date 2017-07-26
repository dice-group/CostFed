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

package com.fluidops.fedx.structures;

import org.eclipse.rdf4j.model.Resource;

import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.repository.RepositoryConnection;

import com.fluidops.fedx.FedX;
import com.fluidops.fedx.FedXConnection;
import com.fluidops.fedx.QueryManager;
import com.fluidops.fedx.optimizer.SourceSelection;
import com.fluidops.fedx.util.QueryStringUtil;



/**
 * Structure to maintain query information during evaluation, is attached to algebra nodes. 
 * Each instance is uniquely attached to the query.
 * 
 * The queryId can be used to abort tasks belonging to a particular evaluation.
 * 
 * @author Andreas Schwarte
 *
 */
public class QueryInfo {

	static AtomicInteger NEXT_QUERY_ID = new AtomicInteger(1);		// static id count
	
	private final FedXConnection conn;
	private final int queryID;
	private final String query;
	private final QueryType queryType;
	private SourceSelection sourceSelection;
	private Object summary;
	
	public int progress = 0;
	public AtomicInteger numSources = new AtomicInteger(0);
	public AtomicInteger totalSources = new AtomicInteger(0);
	
	// for async task priorities
	static ThreadLocal<Integer> priority = new ThreadLocal<Integer>() { @Override protected Integer initialValue() { return 0; } };
	public static ThreadLocal<QueryInfo> queryInfo = new ThreadLocal<QueryInfo>();
	//public static boolean isNJoinchangedOrder = false;
	
	public QueryInfo(FedXConnection conn, String query, QueryType queryType, Object summary) {
		synchronized (QueryInfo.class) {
			this.queryID = NEXT_QUERY_ID.getAndIncrement();
		}
		this.conn = conn;
		this.query = query;
		this.queryType = queryType;
		this.summary = summary;
		queryInfo.set(this);
	}
	
	public QueryInfo(FedXConnection conn, Resource subj, IRI pred, Value obj, Object summary) {
		this(conn, QueryStringUtil.toString(subj, pred, obj), QueryType.GET_STATEMENTS, summary);
	}

	public FedX getFederation() {
	    return conn.getFederation();
	}
	
	public FedXConnection getFedXConnection() {
	    return conn;
	}
	
	public QueryManager getQueryManager() {
	    return conn.getQueryManager();
	}
	
	public int getQueryID() {
		return queryID;
	}

	public String getQuery() {
		return query;
	}	
	
	public QueryType getQueryType() {
		return queryType;
	}

	public SourceSelection getSourceSelection() {
		return sourceSelection;
	}
	
	public void setSourceSelection(SourceSelection sourceSelection) {
		this.sourceSelection = sourceSelection;
	}
	
	@SuppressWarnings("unchecked")
    public <T> T getSummary() {
	    return (T)summary;
	}
	
	RepositoryConnection getSummaryConnection() {
	    throw new RuntimeException("not implemented");
	}
	
	public static int getPriority() {
		return priority.get();
	}
	
	public static void setPriority(int value) {
		priority.set(value);
	}
	
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + queryID;
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		QueryInfo other = (QueryInfo) obj;
		if (queryID != other.queryID)
			return false;
		return true;
	}
	
	
}
