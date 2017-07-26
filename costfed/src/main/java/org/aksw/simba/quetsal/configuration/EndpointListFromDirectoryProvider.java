package org.aksw.simba.quetsal.configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fluidops.fedx.Config;
import com.fluidops.fedx.EndpointListProvider;
import com.fluidops.fedx.FedX;
import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.util.EndpointFactory;

public class EndpointListFromDirectoryProvider implements EndpointListProvider {
    static Logger log = LoggerFactory.getLogger(EndpointListFromDirectoryProvider.class);
    
    List<Endpoint> endpoints = new ArrayList<Endpoint>();
    List<String> strEndpoints = new ArrayList<String>();
    Map<String, Endpoint> cache = new HashMap<String, Endpoint>();
    
    String path;
    WatchService watcher;
    WatchKey watchKey = null;
    Thread watcherThread;
    
    public EndpointListFromDirectoryProvider()
    {
        try {
            watcher = FileSystems.getDefault().newWatchService();
            watcherThread = new Thread(new WatcherProc());
            watcherThread.start();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
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
                        rebuildEndpoints();
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
    public synchronized List<Endpoint> getEndpoints(FedX federation) {
        Config config = federation.getConfig();
        String path = config.getProperty("quetzal.endpoints");
        if (path == null) {
            this.path = null;
            resetEndpoints();
            return doGetEndpoints(federation);
        }
        if (!path.equals(this.path)) {
            this.path = path;
            registerWatcher();
            resetEndpoints();
            rebuildEndpoints();
        }
        return doGetEndpoints(federation);
    }

    List<Endpoint> doGetEndpoints(FedX federation) {
        if (!endpoints.isEmpty()) return endpoints;

        for (String url : strEndpoints) {
            Endpoint e = cache.get(url);
            if (e == null) {
                e = EndpointFactory.loadSPARQLEndpoint(federation.getConfig(), federation.getHttpClient(), url);
                cache.put(url, e);
            }
            endpoints.add(e);
        }
        return endpoints;
    }
    
    @Override
    public void close() {
        try {
            for (Map.Entry<String, Endpoint> entry : cache.entrySet())
            {
                entry.getValue().shutDown();
            }
            cache.clear();
            if (watcherThread != null) {
                watcherThread.interrupt();
                watcherThread.join();
                watcherThread = null;
            }
        } catch (Exception ex) {
            log.error("", ex);
        }
    }

    void resetEndpoints() {
        endpoints.clear();;
        strEndpoints.clear();
    }

    synchronized void registerWatcher() {
        try {
            if (watchKey != null) {
                watchKey.cancel();
                watchKey = null;
            }
            
            Path dir = Paths.get(path);
            watchKey = dir.register(watcher, java.nio.file.StandardWatchEventKinds.ENTRY_CREATE, java.nio.file.StandardWatchEventKinds.ENTRY_DELETE, java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY);

        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
    
    synchronized void rebuildEndpoints() {
        endpoints.clear();
        strEndpoints.clear();
        
        File folder = new File(path);
        
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) continue;
            if (fileEntry.getName().endsWith(".temp")) continue;
            
            Properties prop = loadEndpoint(fileEntry);
            if (prop == null) continue;
            
            boolean enable = "true".equals(prop.getProperty("enable"));
            if (!enable) continue;
            
            boolean hasValidSummary = "ready".equals(prop.getProperty("summary"));
            if (!hasValidSummary) continue;
            
            String address = prop.getProperty("address");
            if (address != null) {
                strEndpoints.add(address);
            }
        }
        log.info("endpoints have been rebuilt: " + strEndpoints);
    }
    
    private Properties loadEndpoint(File file)
    {
        Properties prop = new Properties();
        try {
            InputStream is = new FileInputStream(file);
            try {
                prop.load(is);
            } finally {
                is.close();
            }
        } catch (IOException e) {
            return null;
        }
        return prop;
    }
}
