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

package org.openrdf.sail.nativerdf;

import info.aduna.iteration.CloseableIteration;

import java.io.IOException;

import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.openrdf.query.algebra.evaluation.impl.SimpleEvaluationStrategy;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.sail.SailException;
import org.openrdf.sail.federation.evaluation.RepositoryTripleSource;


/**
 * Extension to Sesame's {@link NativeStoreConnection} to allow for efficient 
 * evaluation of precompiled queries without prior optimizations. When creating
 * the NativeStore the following hook should be used:
 * 
 * <code>
 * NativeStore ns = new NativeStore(store) {
 * 	@Override
 *  protected NotifyingSailConnection getConnectionInternal() throws SailException {
 *    	try {
 *    		return new NativeStoreConnectionExt(this);
 *     	} catch (IOException e) {
 *     		throw new SailException(e);
 *     	}
 *   }};
 * </code>
 * 
 * @author Andreas Schwarte
 *
 */
public class NativeStoreConnectionExt extends NativeStoreConnection {

	public NativeStoreConnectionExt(NativeStore nativeStore)
			throws IOException {
		super(nativeStore);
	}

	
	public final CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluatePrecompiled(
			TupleExpr tupleExpr)
		throws QueryEvaluationException
	{
		connectionLock.readLock().lock();
		try {
			verifyIsOpen();
			return registerIteration(evaluatePrecompiledInternal(tupleExpr, null, null, true));
		} catch (SailException e) {
			throw new QueryEvaluationException(e);
		}
		finally {
			connectionLock.readLock().unlock();
		}
	}
	
	
	
	protected CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluatePrecompiledInternal(
			TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred) {
			/*
			replaceValues(tupleExpr);
	*/
			TripleSource tripleSource = null;
			//new RepositoryTripleSource(nativeStore, includeInferred, transactionActive());
			
			SimpleEvaluationStrategy strategy = new SimpleEvaluationStrategy(tripleSource, dataset, null);
			
			return strategy.evaluate(tupleExpr, EmptyBindingSet.getInstance());
	}
	
}
