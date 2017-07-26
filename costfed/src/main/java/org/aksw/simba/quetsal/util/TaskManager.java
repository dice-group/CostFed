package org.aksw.simba.quetsal.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskManager {
    static Logger log = LoggerFactory.getLogger(TaskManager.class);
    
    ExecutorService executorService;
    List<Thread> threads = new ArrayList<Thread>();
    List<Future<?>> tasks = new ArrayList<Future<?>>();
    
    public TaskManager(int nThreads)
    {
        executorService = Executors.newFixedThreadPool(nThreads, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                threads.add(t);
                return t;
            }
        });
    }
    
    public void interrupt() {
        executorService.shutdown();
        for (Thread t : threads)
        {
            t.interrupt();
        }
        
    }
    
    public void join() {
        for (Thread t : threads)
        {
            try {
                t.join();
            } catch (InterruptedException e) {
                log.error("", e);
            }
        }
    }
    
    public Future<?> addTask(Runnable task) {
        Future<?> future = null;
        synchronized (executorService) {
            future = executorService.submit(task);
            tasks.add(future);
        }
        return future;
    }
    
    public <T> Future<T> addTask(Callable<T> task) {
        Future<T> future = null;
        synchronized (executorService) {
            future = executorService.submit(task);
            tasks.add(future);
        }
        return future;
    }
    
    public void waitForTasks() {
        Future<?> future = null;
        while (true) {
            synchronized (executorService) {
                if (tasks.isEmpty()) return;
                future = tasks.get(0);
            }
            try {
                future.get();
            } catch (Exception e) {
                log.error("", e);
            }
            synchronized (executorService) {
                tasks.remove(0);
            }
        }
    }
}
