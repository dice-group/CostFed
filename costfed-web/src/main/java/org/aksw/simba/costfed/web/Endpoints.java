package org.aksw.simba.costfed.web;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Endpoints {
    static Logger log = LoggerFactory.getLogger(Endpoints.class);
    
    static int maxid = 0;
    static Object mtx = new Object();
    
    Map<String, Properties> cachedEndpoints = new HashMap<String, Properties>();
    
    Properties makeCopy(Properties p)
    {
        if (p == null) return null;
        Properties result = new Properties();
        for (Entry<Object, Object> e : p.entrySet())
        {
            result.put(e.getKey(), e.getValue());
        }
        return result;
    }
    
    public synchronized Properties cget(String id) {
        Properties r = cachedEndpoints.get(id);
        if (r == null) {
            r = loadEndpoint(id);
            if (r != null) {
                cachedEndpoints.put(id, r);
            }
        }
        return r;
    }
    
    public synchronized Properties get(String id) {
        return makeCopy(cget(id));
    }
    
    public synchronized void set(String id, Properties props) throws IOException {
        saveEndpoint(id, props);
        cachedEndpoints.put(id, props);
    }
    
    // update values only for properties in props
    public synchronized Properties update(String id, Properties props, boolean store) throws IOException {
        Properties main = cachedEndpoints.get(id);
        if (main == null) {
            main = loadEndpoint(id);
            if (main == null) return null;
            cachedEndpoints.put(id, main);
        }
        if (main != null) {
            for (Entry<Object, Object> e : props.entrySet())
            {
                main.setProperty((String)e.getKey(), (String)e.getValue());
            }
        
            if (store) saveEndpoint(id, main);
        }
        return makeCopy(main);
    }
    
    public synchronized void remove(String eid)
    {
        // java.nio.file.Files.delete(java.nio.file.Paths.get(Constants.endpointsPath(), eid));
        File file = new File(Constants.endpointsPath() + File.separator + eid);
        file.delete();
        cachedEndpoints.remove(eid);
    }
    
    private Properties loadEndpoint(String eid)
    {
        File file = new File(Constants.endpointsPath() + File.separator + eid);
        
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
    
    private void saveEndpoint(String eid, Properties props) throws IOException
    {
        File file = new File(Constants.endpointsPath() + File.separator + eid);
        File tempfile = new File(Constants.endpointsPath() + File.separator + eid + ".temp");
        
        OutputStream os = new FileOutputStream(tempfile);
        try {
            props.store(os, null);
        } finally {
            os.close();   
        }
        
        file.delete();
        tempfile.renameTo(file);
    }
    
    public static int getMaxId()
    {
        synchronized (mtx) {
            if (maxid == 0) {
                File folder = new File(Constants.endpointsPath());
                for (final File fileEntry : folder.listFiles()) {
                    if (fileEntry.isDirectory()) continue;
                    try {
                        int num = Integer.parseInt(fileEntry.getName());
                        if (num > maxid) {
                            maxid = num;
                        }
                    } catch (NumberFormatException ignore) {
                        continue;
                    }
                }
            }
            maxid++;
            return maxid;
        }
    }
    
    public void writeError(String eid, Throwable error)
    {
        File file = new File(Constants.errorsPath() + File.separator + eid + ".error");
        File tempfile = new File(Constants.errorsPath() + File.separator + eid + ".error.temp");
        
        try {
            OutputStream os = new FileOutputStream(tempfile);
            PrintWriter pwr = new PrintWriter(os);
            try {
                error.printStackTrace(pwr);
            } finally {
                pwr.flush();
                os.close();   
            }
            
            file.delete();
            tempfile.renameTo(file);
        } catch (Exception e) {
            log.error("", e);
        }
    }
    
    public String getError(String eid) throws IOException
    {
        File file = new File(Constants.errorsPath() + File.separator + eid + ".error");
        if (!file.exists()) return null;
        InputStream is = new FileInputStream(file);
        
        try {
            ByteArrayOutputStream result = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int length;
            while ((length = is.read(buffer)) != -1) {
                result.write(buffer, 0, length);
            }
            return result.toString("UTF-8");
        } finally {
            is.close();
        }
    }
    
    public void writeSummary(String eid, String summary) 
    {
        File file = new File(Constants.summariesPath() + File.separator + eid + ".ttl");
        File tempfile = new File(Constants.summariesPath() + File.separator + eid + ".ttl.temp");
        
        try {
            OutputStream os = new FileOutputStream(tempfile);
            try {
                os.write(summary.getBytes("UTF-8"));
            } finally {
                os.close();   
            }
            
            file.delete();
            tempfile.renameTo(file);
        } catch (Exception e) {
            log.error("", e);
        }  
    }
    
    public String getSummary(String eid) throws IOException
    {
        File file = new File(Constants.summariesPath() + File.separator + eid + ".ttl");
        if (!file.exists()) return null;
        InputStream is = new FileInputStream(file);
        
        try {
            ByteArrayOutputStream result = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int length;
            while ((length = is.read(buffer)) != -1) {
                result.write(buffer, 0, length);
            }
            return result.toString("UTF-8");
        } finally {
            is.close();
        }
    }
}
