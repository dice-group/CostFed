package org.aksw.simba.quetsal.core.evaluation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;

import com.fluidops.fedx.evaluation.FederationEvalStrategy;
import com.fluidops.fedx.evaluation.concurrent.Async;
import com.fluidops.fedx.evaluation.concurrent.ControlledWorkerScheduler;
import com.fluidops.fedx.evaluation.iterator.QueueIterator;
import com.fluidops.fedx.evaluation.iterator.RestartableCloseableIteration;
import com.fluidops.fedx.evaluation.iterator.RestartableLookAheadIteration;
import com.fluidops.fedx.structures.QueryInfo;

public class HashJoinImpl extends RestartableLookAheadIteration<BindingSet> {
	public static Logger log = LoggerFactory.getLogger(HashJoinImpl.class);
	
	/* Constants */
	private final ControlledWorkerScheduler scheduler;
	protected final FederationEvalStrategy strategy;		// the evaluation strategy
	protected List<TupleExpr> childExprs;
	protected List<CloseableIteration<BindingSet, QueryEvaluationException>> childIters;
	//protected final TupleExpr leftExpr;
	//protected final TupleExpr rightExpr;
	protected Set<String> joinAttributes;
	protected final BindingSet bindings;					// the bindings
	protected final QueryInfo queryInfo;
	private AtomicBoolean started = new AtomicBoolean(false);
	AtomicInteger runCount = new AtomicInteger(0);
	
	
	protected List<Map<Object, Collection<BindingSet>>> resultTables;
	
	//Map<Object, Collection<T>>
	private QueueIterator<BindingSet> resultQueue =
		new QueueIterator<BindingSet>(new QueueIterator.ItemReleaser<BindingSet>() {
			@Override public void release(BindingSet item) {}
		});
	
	public HashJoinImpl(ControlledWorkerScheduler scheduler, FederationEvalStrategy strategy,
			Set<String> leftArg, TupleExpr leftExpr,
			Set<String> rightArg, TupleExpr rightExpr,
			//Set<String> leftArg, CloseableIteration<BindingSet, QueryEvaluationException> leftIter,
			//Set<String> rightArg, CloseableIteration<BindingSet, QueryEvaluationException> rightIter,
			BindingSet bindings, QueryInfo queryInfo)
	{
		this.strategy = strategy;
		this.bindings = bindings;
		this.queryInfo = queryInfo;
		this.scheduler = scheduler;
		
		// find join attrs intersection
		joinAttributes = new HashSet<String>();
		for (String v : leftArg) {
			if (v.startsWith("_const_")) continue;
			joinAttributes.add(v);
		}
        joinAttributes.retainAll(rightArg);
        log.info("", joinAttributes);
        
        childExprs = new ArrayList<TupleExpr>();
        childExprs.add(leftExpr);
        childExprs.add(rightExpr);
        childIters = new ArrayList<>(2);
        childIters.add(null);
        childIters.add(null);
		resultTables = new ArrayList<Map<Object, Collection<BindingSet>>>();
		resultTables.add(new HashMap<Object, Collection<BindingSet>>());
		resultTables.add(new HashMap<Object, Collection<BindingSet>>());
	}

	//public void addChildIterator(CloseableIteration<BindingSet, QueryEvaluationException> it) {
	//	childIters.add(it);
	//}
	
	public void start() {
		if (started.compareAndSet(false, true)) {
			do_start();
		}
	}
	
    private BindingSet calcKey(BindingSet bindings, Set<String> commonVars) {
        QueryBindingSet q = new QueryBindingSet();
        for (String varName : commonVars) {
            Binding b = bindings.getBinding(varName);
            if (b != null) {
                q.addBinding(b);
            }
        }
        return q;
    }
    
    public static BindingSet combineBindings(BindingSet b1, BindingSet b2) {
        QueryBindingSet result = new QueryBindingSet();
        for (Binding b : b1) {
            if (!result.hasBinding(b.getName())) {
                result.addBinding(b);
            }
        }

        for (String name : b2.getBindingNames()) {
            Binding b = b2.getBinding(name);
            if (!result.hasBinding(name)) {
                result.addBinding(b);
            }
        }

        return result;
    }
    
	static class KeyAndValue {
		BindingSet key;
		BindingSet value;
		public KeyAndValue(BindingSet key, BindingSet value) {
			super();
			this.key = key;
			this.value = value;
		}
	}

	class InnerCallable implements Callable<CloseableIteration<BindingSet, QueryEvaluationException>>
	{
		final int idx;
		public InnerCallable(int idx) {
			this.idx = idx;
		}
		
		@Override
		public CloseableIteration<BindingSet, QueryEvaluationException> call() throws Exception {
			CloseableIteration<BindingSet, QueryEvaluationException> it = childIters.get(idx);
			if (it == null) {
				it = strategy.evaluate(childExprs.get(idx), bindings);
				childIters.set(idx, it);
			}
			return it;
		}
	}
	
	public class HashJoinTask extends Async<CloseableIteration<BindingSet, QueryEvaluationException>> {
		final int idx;

		HashJoinTask(int idx) {
			super(new InnerCallable(idx));
			this.idx = idx;
			resultQueue.onAddIterator();
			runCount.incrementAndGet();
		}
		
		Collection<KeyAndValue> buff = new ArrayList<KeyAndValue>();
		
		@Override
		public void callAsync(CloseableIteration<BindingSet, QueryEvaluationException> res) {

			int gotRecs = 0;
			int putRecs = 0;
			

			
			while (res.hasNext() && !isClosed()) {
				if (true || runCount.get() > 1) {
					buff.clear();
					for (int i = 0; i < 100 && res.hasNext(); ++i) {
						BindingSet r = res.next();
						BindingSet hashKey = calcKey(r, joinAttributes);
						buff.add(new KeyAndValue(hashKey, r));

					}
				
					synchronized (resultTables) {
						for (KeyAndValue kv : buff)
						{
							++gotRecs;

							Collection<BindingSet> coll = resultTables.get(idx).get(kv.key);
							if (coll == null) {
								coll = new ArrayList<BindingSet>();
								resultTables.get(idx).put(kv.key, coll);
							}
							coll.add(kv.value);
							Collection<BindingSet> fcoll = resultTables.get(1 - idx).get(kv.key);
							if (fcoll != null) {
								for (BindingSet fs : fcoll) {
									resultQueue.simple_add(combineBindings(kv.value, fs));
									putRecs++;
								}
							}
						}
					}
				} else {
					//resultTables.get(idx).clear(); // free unnecessary table
					Map<Object, Collection<BindingSet>> coll = resultTables.get(1 - idx);
					for (int i = 0; i < 100 && res.hasNext(); ++i) {
						BindingSet r = res.next();
						++gotRecs;
						BindingSet hashKey = calcKey(r, joinAttributes);
						Collection<BindingSet> fcoll = coll.get(hashKey);
						if (fcoll != null) {
							for (BindingSet fs : fcoll) {
								resultQueue.simple_add(combineBindings(r, fs));
								putRecs++;
							}
						}
					}
				}
			}
			resultQueue.onRemoveIterator();
			runCount.decrementAndGet();
			log.info("hash: " + HashJoinImpl.this.hashCode() + ", idx: " + idx + ", got records: " + gotRecs + ", put records: " + putRecs + ", closed: " + isClosed());
			res.close();
		}

		@Override
		public void exception(Exception e) {
			log.warn("Error executing union operator: " + e.getMessage());
			resultQueue.add(e);
		}
	}
	
	protected void do_start() {
		for (int idx = 0; idx < childExprs.size(); ++idx) {
			scheduler.schedule(new HashJoinTask(idx));
		}
	}
	
	@Override
	protected BindingSet getNextElement() {
		if (!started.get()) {
			start();
		}
		if (resultQueue.hasNext()) {
			return resultQueue.next();
		}
		return null;
	}
	
	@Override
	public void handleClose() {
		resultQueue.close();
		for (CloseableIteration<BindingSet, QueryEvaluationException> iter : childIters) {
			iter.close();
		}
		super.handleClose();
	}
	
	@Override
	public void handleRestart() {
		resultQueue.restart();
		for (int idx = 0; idx < childIters.size(); ++idx) {
			if (childIters.get(idx) instanceof RestartableCloseableIteration) {
				((RestartableCloseableIteration<BindingSet>)childIters.get(idx)).restart();
				scheduler.schedule(new HashJoinTask(idx));
			}
		}
	}
}
