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

package com.fluidops.fedx.evaluation.iterator;

import org.eclipse.rdf4j.common.iteration.AbstractCloseableIteration;

import java.util.NoSuchElementException;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.RepositoryResult;


/**
 * Converts Statement iteration (i.e. RepositoryResult) into the corresponding binding set. Note that
 * exceptions are converted appropriately as well.
 * 
 * @author Andreas Schwarte
 */
public class StatementConversionIteration extends AbstractCloseableIteration<BindingSet, QueryEvaluationException>{

	protected final RepositoryResult<Statement> repoResult;
	protected final BindingSet bindings;
	protected final StatementPattern stmt;
	
	protected boolean updateSubj = false;
	protected boolean updatePred = false;
	protected boolean updateObj = false;
	protected boolean updateContext = false;
	
	public StatementConversionIteration(RepositoryResult<Statement> repoResult,
			BindingSet bindings, StatementPattern stmt) {
		super();
		this.repoResult = repoResult;
		this.bindings = bindings;
		this.stmt = stmt;
		init();
	}
	
	protected void init() {
		updateSubj = stmt.getSubjectVar() != null && !bindings.hasBinding(stmt.getSubjectVar().getName());
		updatePred = stmt.getPredicateVar() != null && !bindings.hasBinding(stmt.getPredicateVar().getName());
		updateObj = stmt.getObjectVar() != null && !bindings.hasBinding(stmt.getObjectVar().getName());
		updateContext = stmt.getContextVar() != null && !bindings.hasBinding(stmt.getContextVar().getName());
	}
	
	@Override
	public boolean hasNext() throws QueryEvaluationException {
		try {
			return repoResult.hasNext();
		} catch (RepositoryException e) {
			throw convertException(e);
		}
	}

	@Override
	public BindingSet next() throws QueryEvaluationException {

		try {
			return convert(repoResult.next());
		} catch(NoSuchElementException e) {
			throw e;
	    } catch(IllegalStateException e) {
	    	throw e;
	    } catch (RepositoryException e) {
			throw convertException(e);
		}
	}

	@Override
	public void remove() throws QueryEvaluationException {

		try {
			repoResult.remove();
		} catch(UnsupportedOperationException e) {
			throw e;
		} catch(IllegalStateException e) {
			throw e;
		} catch (RepositoryException e) {
			throw convertException(e);
		}
		
	}

	
	protected BindingSet convert(Statement st) {
		QueryBindingSet result = new QueryBindingSet(bindings);

		if (updateSubj) {
			result.addBinding(stmt.getSubjectVar().getName(), st.getSubject());
		}
		if (updatePred) {
			result.addBinding(stmt.getPredicateVar().getName(), st.getPredicate());
		}
		if (updateObj) {
			result.addBinding(stmt.getObjectVar().getName(), st.getObject());
		}
		if (updateContext && st.getContext() != null) {
			result.addBinding(stmt.getContextVar().getName(), st.getContext());
		}

		return result;
	}
	
	protected QueryEvaluationException convertException(Exception e) {
		return new QueryEvaluationException(e);
	}
	
}
