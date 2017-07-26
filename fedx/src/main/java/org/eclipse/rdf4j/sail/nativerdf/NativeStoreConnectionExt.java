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

package org.eclipse.rdf4j.sail.nativerdf;

import java.io.IOException;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategy;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.sail.SailException;


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
			
			StrictEvaluationStrategy strategy = new StrictEvaluationStrategy(tripleSource, dataset, null);
			
			return strategy.evaluate(tupleExpr, EmptyBindingSet.getInstance());
	}
	
}
