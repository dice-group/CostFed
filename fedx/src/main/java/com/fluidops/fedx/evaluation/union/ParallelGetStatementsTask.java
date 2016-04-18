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

package com.fluidops.fedx.evaluation.union;

import java.util.concurrent.Callable;

import org.openrdf.model.IRI;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.repository.RepositoryConnection;

import com.fluidops.fedx.evaluation.TripleSource;

import info.aduna.iteration.CloseableIteration;

/**
 * A task implementation to retrieve statements for a given {@link StatementPattern}
 * using the provided triple source.
 * 
 * @author Andreas Schwarte
 */
public class ParallelGetStatementsTask implements Callable<CloseableIteration<Statement, QueryEvaluationException>> {

	protected final Resource subj;
	protected final IRI pred;
	protected final Value obj;
	protected Resource[] contexts;
	protected final TripleSource tripleSource;
	protected final RepositoryConnection conn;
		
	public ParallelGetStatementsTask(
			TripleSource tripleSource, RepositoryConnection conn,
			Resource subj, IRI pred, Value obj, Resource... contexts)
	{	
		this.tripleSource = tripleSource;
		this.conn = conn;
		this.subj = subj;
		this.pred = pred;
		this.obj = obj;
		this.contexts = contexts;		
	}

	@Override
	public CloseableIteration<Statement, QueryEvaluationException> call() {
		return tripleSource.getStatements(conn, subj, pred, obj, contexts);
	}
}
