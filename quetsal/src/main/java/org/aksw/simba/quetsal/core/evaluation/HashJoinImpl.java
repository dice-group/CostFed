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

import org.apache.log4j.Logger;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import com.fluidops.fedx.evaluation.concurrent.Async;
import com.fluidops.fedx.evaluation.concurrent.ControlledWorkerScheduler;
import com.fluidops.fedx.evaluation.iterator.QueueIterator;
import com.fluidops.fedx.structures.QueryInfo;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.LookAheadIteration;

public class HashJoinImpl extends LookAheadIteration<BindingSet, QueryEvaluationException> {
	public static Logger log = Logger.getLogger(HashJoinImpl.class);
	
	/* Constants */
	private final ControlledWorkerScheduler scheduler;
	protected Set<String> joinAttributes;
	protected final BindingSet bindings;					// the bindings
	protected final QueryInfo queryInfo;
	private AtomicBoolean started = new AtomicBoolean(false);
	AtomicInteger runCount = new AtomicInteger(0);
	
	protected List<CloseableIteration<BindingSet, QueryEvaluationException>> childIters;
	protected List<Map<Object, Collection<BindingSet>>> resultTables;
	
	//Map<Object, Collection<T>>
	private QueueIterator<BindingSet> resultQueue =
		new QueueIterator<BindingSet>(new QueueIterator.ItemReleaser<BindingSet>() {
			@Override public void release(BindingSet item) {}
		});
	
	public HashJoinImpl(
			ControlledWorkerScheduler scheduler,
			Set<String> leftArg, CloseableIteration<BindingSet, QueryEvaluationException> leftIter,
			Set<String> rightArg, CloseableIteration<BindingSet, QueryEvaluationException> rightIter,
			BindingSet bindings, QueryInfo queryInfo)
	{
		this.bindings = bindings;
		this.queryInfo = queryInfo;
		this.scheduler = scheduler;
		
		// find join attrs intersection
		joinAttributes = new HashSet<String>(leftArg);
        joinAttributes.retainAll(rightArg);
        assert(joinAttributes.size() > 0);

		childIters = new ArrayList<CloseableIteration<BindingSet, QueryEvaluationException>>();
		childIters.add(leftIter);
		childIters.add(rightIter);
		resultTables = new ArrayList<Map<Object, Collection<BindingSet>>>();
		resultTables.add(new HashMap<Object, Collection<BindingSet>>());
		resultTables.add(new HashMap<Object, Collection<BindingSet>>());
		runCount.set(resultTables.size());
	}

	public void addChildIterator(CloseableIteration<BindingSet, QueryEvaluationException> it) {
		childIters.add(it);
	}
	
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
	
	public class HashJoinTask extends Async<CloseableIteration<BindingSet, QueryEvaluationException>> {
		final int idx;
		HashJoinTask(int idx) {
			super(new Callable<CloseableIteration<BindingSet, QueryEvaluationException>>(){
				@Override
				public CloseableIteration<BindingSet, QueryEvaluationException> call() throws Exception {
					return childIters.get(idx);
				}
			});
			this.idx = idx;
			resultQueue.onAddIterator();
		}
		
		@Override
		public void run() {
			QueryInfo.setPriority(1);
			super.run();
		}
		

		
		Collection<KeyAndValue> buff = new ArrayList<KeyAndValue>();
		
		@Override
		public void callAsync(CloseableIteration<BindingSet, QueryEvaluationException> res) {
			int gotRecs = 0;
			int putRecs = 0;
			
			while (res.hasNext() && !isClosed()) {
				if (runCount.get() > 1) {
					buff.clear();
					for (int i = 0; i < 100 && res.hasNext(); ++i) {
						BindingSet r = res.next();
						BindingSet hashKey = calcKey(r, joinAttributes);
						if (hashKey.size() == 0) {
							assert(hashKey.size() != 0);
						}
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
					resultTables.get(idx).clear(); // free unnecessary table
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
			log.info("idx: " + idx + ", got records: " + gotRecs + ", put records: " + putRecs + ", closed: " + isClosed());
			res.close();
		}

		@Override
		public void exception(Exception e) {
			log.warn("Error executing union operator: " + e.getMessage());
			resultQueue.add(e);
		}
	}
	
	protected void do_start() {
		for (int idx = 0; idx < childIters.size(); ++idx) {
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
}
