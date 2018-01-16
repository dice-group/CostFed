package org.aksw.simba.quetsal.configuration;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fluidops.fedx.Config;
import com.fluidops.fedx.FedX;
import com.fluidops.fedx.SummaryProvider;

public class CostFedSummaryProvider implements SummaryProvider {
    static Logger log = LoggerFactory.getLogger(CostFedSummaryProvider.class);
    
    String path;
    CostFedSummary summary = null;
    WatchService watcher;
    WatchKey watchKey = null;
    Thread watcherThread;
    
    public CostFedSummaryProvider() {
        try {
            watcher = FileSystems.getDefault().newWatchService();
            watcherThread = new Thread(new WatcherProc());
            watcherThread.start();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
    
    @Override
    public Summary getSummary(FedX federation) {
        Config config = federation.getConfig();
        String path = config.getProperty("quetzal.fedSummaries");
        if (path == null) {
            this.path = null;
            resetSummary();
            return summary;
        }
        if (!path.equals(this.path)) {
            this.path = path;
            registerWatcher();
            resetSummary();
            rebuildSummary();
        }
        return doGetSummary(); // to enable synchronized access
    }

    synchronized void resetSummary() {
        if (summary != null) {
            summary.close();
            summary = null;
        }
    }
    
    synchronized void registerWatcher() {
        try {
            if (watchKey != null) {
                watchKey.cancel();
                watchKey = null;
            }
            
            File file = new File(path).getCanonicalFile();
            if (file.isDirectory()) {
                Path dir = Paths.get(path);
                watchKey = dir.register(watcher, java.nio.file.StandardWatchEventKinds.ENTRY_CREATE, java.nio.file.StandardWatchEventKinds.ENTRY_DELETE, java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY);
            } else {
                Path dir = Paths.get(file.getParent());
                watchKey = dir.register(watcher, java.nio.file.StandardWatchEventKinds.ENTRY_CREATE, java.nio.file.StandardWatchEventKinds.ENTRY_DELETE, java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
    
    synchronized Summary doGetSummary() {
        if (summary != null) {
            summary.addref();
        }
        return summary;
    }
    
    synchronized void rebuildSummary() {
        try {
            doRebuildSummary();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
    
    void doRebuildSummary() throws Exception {
        try {
            
            List<String> files = new ArrayList<String>();
            File file = new File(path);
            if (file.isDirectory()) {
                
                for (final File fileEntry : file.listFiles()) {
                    if (fileEntry.isDirectory()) continue;
                    if (!fileEntry.getName().endsWith(".ttl") && !fileEntry.getName().endsWith(".n3")) continue;
                    files.add(fileEntry.getPath());
                }
            } else {
                log.info(file.getAbsoluteFile().getPath());
                files.add(path);
            }
            
            log.info("build summary for: " + files);
            CostFedSummary tempSummary = new CostFedSummary(files);
            if (summary != null) {
                summary.close();
            }
            summary = tempSummary;
        } catch (Throwable t) {
            log.error("can't load summary", t);
        }
    }
    
    class WatcherProc implements Runnable {
        @Override
        public void run() {
            for (;;) {
                WatchKey key;
                try {
                    key = watcher.take();
                } catch (InterruptedException x) {
                    return;
                }
                
                for (WatchEvent<?> event: key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();
                    if (kind == java.nio.file.StandardWatchEventKinds.OVERFLOW) {
                        continue;
                    }
                    
                    if (kind == java.nio.file.StandardWatchEventKinds.ENTRY_CREATE || kind == java.nio.file.StandardWatchEventKinds.ENTRY_DELETE || kind == java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY) {
                        rebuildSummary();
                    }
                }
                
                boolean valid = key.reset();
                if (!valid) {
                    break;
                }
            }
        }
    }

    @Override
    public synchronized void close() {
        try {
            if (watchKey != null) {
                watchKey.cancel();
            }
            if (watcherThread != null) {
                watcherThread.interrupt();
                watcherThread.join();
                watcherThread = null;
            }
        } catch (Exception ex) {
            log.error("", ex);
        }
    }
}
