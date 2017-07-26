package org.aksw.simba.quetsal.core.algebra;

import java.util.List;

import org.eclipse.rdf4j.query.algebra.TupleExpr;

import com.fluidops.fedx.algebra.NJoin;
import com.fluidops.fedx.structures.QueryInfo;

public class NHashJoin extends NJoin {
	private static final long serialVersionUID = 3098362185680275241L;

	/**
	 * Construct an nary-tuple. Note that the parentNode of all arguments is
	 * set to this instance.
	 * 
	 * @param args
	 */
	public NHashJoin(List<TupleExpr> args, QueryInfo queryInfo) {
		super(args, queryInfo);
	}
}
