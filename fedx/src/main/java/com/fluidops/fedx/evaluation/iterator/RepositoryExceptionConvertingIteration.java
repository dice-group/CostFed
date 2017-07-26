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

import org.eclipse.rdf4j.common.iteration.ExceptionConvertingIteration;
import org.eclipse.rdf4j.common.iteration.Iteration;

import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.repository.RepositoryResult;


/**
 * Convenience iteration to convert {@link RepositoryResult} exceptions to {@link QueryEvaluationException}.
 *  
 * @author Andreas Schwarte
 *
 * @param <T>
 */
public class RepositoryExceptionConvertingIteration<T> extends ExceptionConvertingIteration<T, QueryEvaluationException> {

	public RepositoryExceptionConvertingIteration(
			Iteration<? extends T, ? extends Exception> iter) {
		super(iter);
	}

	@Override
	protected QueryEvaluationException convert(Exception e) {
		return new QueryEvaluationException(e);
	}
}
