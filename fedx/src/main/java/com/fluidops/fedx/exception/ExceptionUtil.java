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

package com.fluidops.fedx.exception;

import java.lang.reflect.Constructor;
import java.net.SocketException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import com.fluidops.fedx.EndpointManager;
import com.fluidops.fedx.structures.Endpoint;

/**
 * Convenience functions to handle exceptions.
 * 
 * @author Andreas Schwarte
 *
 */
public class ExceptionUtil {

	protected static Logger log = Logger.getLogger(ExceptionUtil.class);
	
	/**
	 * Regex pattern to identify http error codes from the title of the returned document:
	 * 
	 * <code>
	 * Matcher m = httpErrorPattern.matcher("[..] <title>503 Service Unavailable</title> [..]");
	 * if (m.matches()) {
	 * 		System.out.println("HTTP Error: " + m.group(1);
	 * }
	 * </code>
	 */
	protected static Pattern httpErrorPattern = Pattern.compile(".*<title>(.*)</title>.*", Pattern.DOTALL);
	
	
	/**
	 * Trace the exception source within the exceptions to identify the originating endpoint. The message
	 * of the provided exception is adapted to "@ endpoint.getId() - %orginalMessage".<p>
	 * 
	 * Note that in addition HTTP error codes are extracted from the title, if the exception resulted from
	 * an HTTP error, such as for instance "503 Service unavailable"
	 * 
	 * @param conn
	 * 			the connection to identify the the endpoint
	 * @param ex
	 * 			the exception
	 * @param additionalInfo
	 * 			additional information that might be helpful, e.g. the subquery
	 * 
	 * @return
	 * 		 	a modified exception with endpoint source
	 */
	public static QueryEvaluationException traceExceptionSource(RepositoryConnection conn, QueryEvaluationException ex, String additionalInfo) {
		
		Endpoint e = EndpointManager.getEndpointManager().getEndpoint(conn);
		
		String eID;
		
		if (e==null) {
			log.warn("No endpoint found for connection, probably changed from different thread.");
			eID = "unknown";
		} else {
			eID = e.getId();
		}
		
		// check for http error code (heuristic)
		String message = ex.getMessage();
		message = message==null ? "n/a" : message;
		Matcher m = httpErrorPattern.matcher(message);
		if (m.matches()) {
			log.error("HTTP error detected for endpoint " + eID + ":\n" + message);
			message = "HTTP Error: " + m.group(1);
		} else {
			log.info("No http error found, found: " + message);
		}

		
		QueryEvaluationException res = new QueryEvaluationException("@ " + eID + " - " + message + ". " + additionalInfo, ex.getCause());
		res.setStackTrace(ex.getStackTrace());
		return res;
	}
	
	
	/**
	 * Repair the connection and then trace the exception source.
	 * 
	 * @param conn
	 * @param ex
	 * @return
	 */
	public static QueryEvaluationException traceExceptionSourceAndRepair(RepositoryConnection conn, QueryEvaluationException ex, String additionalInfo) {
		repairConnection(conn, ex);
		return traceExceptionSource(conn, ex, additionalInfo);
	}
	
	/**
	 * Walk the stack trace and in case of SocketException repair the connection of the
	 * particular endpoint.
	 * 
	 * @param conn
	 * 			the connection to identify the endpoint
	 * @param ex
	 * 			the exception
	 * 
	 * @throws FedXRuntimeException
	 * 				if the connection could not be repaired
	 */
	public static void repairConnection(RepositoryConnection conn, Exception ex) throws FedXQueryException, FedXRuntimeException {

		Throwable cause = ex.getCause();
		while (cause != null) {
			if (cause instanceof SocketException) {
				try {
					Endpoint e = EndpointManager.getEndpointManager().getEndpoint(conn);
					EndpointManager.getEndpointManager().repairAllConnections();
					throw new FedXQueryException("Socket exception occured for endpoint " + getExceptionString(e==null?"unknown":e.getId(), ex) + ", all connections have been repaired. Query processing of the current query is aborted.", cause);
				} catch (RepositoryException e) {
					log.error("Connection could not be repaired: ", e);
					throw new FedXRuntimeException(e.getMessage(), e);
				}				
			}
			cause = cause.getCause();
		}
	}
	
	/**
	 * Return the exception in a convenient representation, i.e. '%msg% (%CLASS%): %ex.getMessage()%'
	 * 
	 * @param msg
	 * @param ex
	 * 
	 * @return
	 * 		the exception in a convenient representation
	 */
	public static String getExceptionString(String msg, Exception ex) {
		return msg + " " + ex.getClass().getSimpleName() + ": " + ex.getMessage();
	}
	
	
	/**
	 * If possible change the message text of the specified exception. This is only possible
	 * if the provided exception has a public constructor with String and Throwable as argument.
	 * The new message is set to 'msgPrefix. ex.getMessage()', all other exception elements 
	 * remain the same.
	 * 
	 * @param <E>
	 * @param msgPrefix
	 * @param ex
	 * @param exClazz
	 * 
	 * @return
	 */
	public static <E extends Exception> E changeExceptionMessage(String msgPrefix, E ex, Class<E> exClazz) {
		
		Constructor<E> constructor = null;
		
		try {
			// try to find the constructor 'public Exception(String, Throwable)'
			constructor = exClazz.getConstructor(new Class<?>[] {String.class, Throwable.class});
		} catch (SecurityException e) {
			log.warn("Cannot change the message of exception class " + exClazz.getCanonicalName() + " due to SecurityException: " + e.getMessage());
			return ex;
		} catch (NoSuchMethodException e) {
			log.warn("Cannot change the message of exception class " + exClazz.getCanonicalName() + ": Constructor <String, Throwable> not found.");
			return ex;
		}
		
		
		E newEx;
		try {
			newEx = constructor.newInstance(new Object[] {msgPrefix + "." + ex.getMessage(), ex.getCause()});
		} catch (Exception e) {
			log.warn("Cannot change the message of exception class " + exClazz.getCanonicalName() + " due to " + e.getClass().getSimpleName() + ": " + e.getMessage());
			return ex;
		}
		newEx.setStackTrace(ex.getStackTrace());
		
		return newEx;
	}
}
