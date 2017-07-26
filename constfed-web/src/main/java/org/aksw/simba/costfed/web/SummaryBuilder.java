package org.aksw.simba.costfed.web;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.aksw.simba.quetsal.util.SummaryGenerator;
import org.aksw.simba.quetsal.util.SummaryGenerator.ProgressListener;
import org.aksw.simba.quetsal.util.TaskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SummaryBuilder {
    static Logger log = LoggerFactory.getLogger(SummaryBuilder.class);
    
    private final static int GENERATOR_THREAD_COUNT = 4;
    private final static int branchLimit = 4;
    
    static class GeneratorDescriptor implements ProgressListener, Runnable
    {
        Thread mainThread;
        TaskManager tm = new TaskManager(GENERATOR_THREAD_COUNT);
        AtomicInteger progress = new AtomicInteger(0);
        int progress_max = 1;
        
        String eid;
        String endpoint;
        Endpoints endpointManager; 
        
        Properties uprops = new Properties();
        
        @Override
        public void setProgress(int val) {
            progress.set(val);
            updateProgress(false);
        }
        
        @Override
        public void setProgressMax(int val) {
            progress_max = val;
            updateProgress(false);
        }

        @Override
        public void incrementProgress() {
            progress.incrementAndGet();
            updateProgress(false);
        }
        
        void updateProgress(boolean store) {
            uprops.setProperty("summary-progress", (int)(100 * progress.get() / progress_max) + "");
            try {
                endpointManager.update(eid, uprops, store);
            } catch (IOException e) { log.error("", e); }
        }
        
        public GeneratorDescriptor(String eid, String endpoint, Endpoints endpointManager)
        {
            this.eid = eid;
            this.endpoint = endpoint;
            this.endpointManager = endpointManager;
        }
        
        @Override
        public void run()
        {
            String result = null;
            uprops.setProperty("summary", "building");
            updateProgress(false);
            try {
                result = SummaryGenerator.generateSummary(endpoint, null, branchLimit, tm, this);
            } catch (Throwable e) {
                endpointManager.writeError(eid, e);
                uprops.setProperty("summary", "error");
                updateProgress(true);
                return;
            } finally {
                tm.interrupt();
                tm.join();
                synchronized (builders) {
                    builders.remove(eid);
                }
            }
            endpointManager.writeSummary(eid, result);
            uprops.setProperty("summary", "ready");
            updateProgress(true);
        }
        
        void interrupt() {
            tm.interrupt();
        }
    }
    
    static Map<String, GeneratorDescriptor> builders = new HashMap<String, GeneratorDescriptor>();
    
    public static void startBuilder(String eid, Endpoints endpointManager)
    {
        synchronized (builders) {
            GeneratorDescriptor gd = builders.get(eid);
            if (gd != null) return; // already started
            Properties props = endpointManager.get(eid);
            if (props == null) return;
            gd = new GeneratorDescriptor(eid, props.getProperty("address"), endpointManager);
            builders.put(eid, gd);
            
            Thread t = new Thread(gd);
            t.start();
        }
    }
    
    public static synchronized void stopBuilder(String eid)
    {
        GeneratorDescriptor gd;
        synchronized (builders) {
            gd = builders.get(eid);
            if (gd == null) return; // already stopped
        }
        gd.interrupt();
    }
}
