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

package com.fluidops.fedx.evaluation;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;

import com.fluidops.fedx.algebra.ExclusiveGroup;
import com.fluidops.fedx.algebra.FilterValueExpr;
import com.fluidops.fedx.structures.QueryType;


/**
 * Interface for implementations of triple sources. Particular implementations define
 * how to evaluate the expression on the endpoint. Different implementations might
 * be necessary depending on the underlying repository.
 * 
 * @author Andreas Schwarte
 *
 * @see SparqlTripleSource
 * @see SailTripleSource
 */
public interface TripleSource {

	
	/**
	 * Evaluate the prepared query in its internal representation on the provided endpoint.
	 * 
	 * @param preparedQuery
	 * 			a prepared query to evaluate
	 * @param conn
	 * 			the connection to the endpoint
	 * @param bindings
	 * 			the bindings to use
	 * @param filterExpr
	 * 			the filter expression to apply or null if there is no filter or if it is evaluated already
	 * 
	 * @return
	 * 		the resulting iteration
	 *  
	 * @throws RepositoryException
	 * @throws MalformedQueryException
	 * @throws QueryEvaluationException
	 */
	public CloseableIteration<BindingSet, QueryEvaluationException> getStatements(TupleExpr preparedQuery, RepositoryConnection conn, final BindingSet bindings, FilterValueExpr filterExpr) throws RepositoryException, MalformedQueryException, QueryEvaluationException;

	
	/**
	 * Evaluate the prepared query (SPARQL query as String) on the provided endpoint.
	 * 
	 * @param preparedQuery
	 * 			a prepared query to evaluate (SPARQL query as String)
	 * @param conn
	 * 			the connection to the endpoint
	 * @param bindings
	 * 			the bindings to use
	 * @param filterExpr
	 * 			the filter expression to apply or null if there is no filter or if it is evaluated already
	 * 
	 * @return
	 * 		the resulting iteration
	 *  
	 * @throws RepositoryException
	 * @throws MalformedQueryException
	 * @throws QueryEvaluationException
	 */
	public CloseableIteration<BindingSet, QueryEvaluationException> getStatements(String preparedQuery, RepositoryConnection conn, final BindingSet bindings, FilterValueExpr filterExpr) throws RepositoryException, MalformedQueryException, QueryEvaluationException;

	/**
	 * Evaluate a given SPARQL query of the provided query type at the given source.
	 * 
	 * @param preparedQuery
	 * @param conn
	 * @param queryType
	 * @return
	 * @throws RepositoryException
	 * @throws MalformedQueryException
	 * @throws QueryEvaluationException
	 */
	public CloseableIteration<BindingSet, QueryEvaluationException> getStatements(String preparedQuery, RepositoryConnection conn, QueryType queryType) throws RepositoryException, MalformedQueryException, QueryEvaluationException;
	
	/**
	 * Evaluate the query expression on the provided endpoint.
	 * 
	 * @param stmt
	 * 			the stmt expression to evaluate
	 * @param conn
	 * 			the connection to the endpoint
	 * @param bindings
	 * 			the bindings to use
	 * @param filterExpr
	 * 			the filter expression to apply or null if there is no filter or if it is evaluated already
	 * 
	 * @return
	 * 		the resulting iteration
	 * 	
	 * @throws RepositoryException
	 * @throws MalformedQueryException
	 * @throws QueryEvaluationException
	 */
	public CloseableIteration<BindingSet, QueryEvaluationException> getStatements(StatementPattern stmt, RepositoryConnection conn, final BindingSet bindings, FilterValueExpr filterExpr) throws RepositoryException, MalformedQueryException, QueryEvaluationException;

	
	/**
	 * Return the statements matching the given pattern as a {@link Statement} iteration.
	 * 
	 * @param stmt
	 * 			the stmt expression to evaluate
	 * @param conn
	 * 			the connection to the endpoint
	 * 
	 * @return
	 * 			the resulting itereation
	 * 
	 * @throws RepositoryException
	 * @throws MalformedQueryException
	 * @throws QueryEvaluationException
	 */
	public CloseableIteration<Statement, QueryEvaluationException> getStatements(RepositoryConnection conn, Resource subj, IRI pred, Value obj, Resource... contexts) throws RepositoryException, MalformedQueryException, QueryEvaluationException;
	
	
	/**
	 * Check if the provided statement can return results.
	 * 
	 * @param stmt
	 * @param conn
	 * @param bindings
	 * 			a binding set. in case no bindings are present, an {@link EmptyBindingSet} can be used (i.e. never null)
	 * 
	 * @return
	 * @throws RepositoryException
	 * @throws MalformedQueryException
	 * @throws QueryEvaluationException
	 */
	public boolean hasStatements(StatementPattern stmt, RepositoryConnection conn, BindingSet bindings) throws RepositoryException, MalformedQueryException, QueryEvaluationException;
	
	/**
	 * Check if the repository can return results for the given triple pattern represented
	 * by subj, pred and obj
	 * 
	 * @param conn
	 * @param subj
	 * @param pred
	 * @param obj
	 * @param contexts
	 * @return
	 * @throws RepositoryException
	 */
	public boolean hasStatements(RepositoryConnection conn, Resource subj, IRI pred, Value obj, Resource...contexts) throws RepositoryException;
	
	/**
	 * Check if the repository can return results for the given {@link ExclusiveGroup},
	 * i.e. a list of Statements
	 * 
	 * @param conn
	 * @param subj
	 * @param pred
	 * @param obj
	 * @param contexts
	 * @return
	 * @throws RepositoryException
	 */
	public boolean hasStatements(ExclusiveGroup group, RepositoryConnection conn, BindingSet bindings) throws RepositoryException, MalformedQueryException, QueryEvaluationException;
	
	
	/**
	 * 
	 * @return
	 * 		true if a prepared query is to be used preferrably, false otherwise
	 */
	public boolean usePreparedQuery();
	
	
}
