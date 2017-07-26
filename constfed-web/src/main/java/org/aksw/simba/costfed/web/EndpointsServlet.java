package org.aksw.simba.costfed.web;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.gson.Gson;

@WebServlet("/endpoints")
public class EndpointsServlet extends HttpServlet {
    private static final long serialVersionUID = -6440455914351636757L;

    static final String ADDRESS_PROP = "address";
    
    static final String SUMMARY_PROP = "summary";
    static final String NO_SUMMARY_VAL = "no";
    static final String READY_SUMMARY_VAL = "ready";
    static final String BUILDING_SUMMARY_VAL = "building";
    
    static final String SUMMARY_PROGRESS_PROP = "summary-progress";
    
    Endpoints endpoints = new Endpoints();
            
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setHeader("Access-Control-Allow-Origin", "*");
        //Access-Control-Allow-Headers=Pragma,Cache-Control,If-Modified-Since,Expires

        String act = request.getParameter("action");
        if ("add".equals(act)) {
            doAddEndpoint(request);
        } else if ("edit".equals(act)) {
            doUpdateEndpoint(request);
        } else if ("toggle".equals(act)) {
            doToggleEndpoint(request);
        } else if ("remove".equals(act)) {
            doRemoveEndpoint(request);
        } else if ("build-summary".equals(act)) {
            doBuildEndpointSummary(request);
        } else if ("stop-build-summary".equals(act)) {
            doStopBuildEndpointSummary(request);
        } else if ("get-error".equals(act)) {
            doGetError(request, response);
            return;
        } else if ("get-summary".equals(act)) {
            doGetSummary(request, response);
            return;
        }
        
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        response.getWriter().println(listEndpoints());
    }
    
    @Override
    public void init() throws ServletException {
        System.out.println("Servlet " + this.getServletName() + " has started");
    }
    
    @Override
    public void destroy() {
        System.out.println("Servlet " + this.getServletName() + " has stopped");
    }
    
    private void doAddEndpoint(HttpServletRequest reqest) throws IOException
    {
        String addr = reqest.getParameter("address");
        String descr = reqest.getParameter("description");
        
        // find max id
        int maxid = Endpoints.getMaxId();
        
        Properties props = new Properties();
        props.setProperty(ADDRESS_PROP, addr);
        props.setProperty("description", descr);
        props.setProperty("enable", "true");
        props.setProperty(SUMMARY_PROP, NO_SUMMARY_VAL);
        props.setProperty(SUMMARY_PROGRESS_PROP, "0");
        
        endpoints.set(maxid + "", props);
    }
    
    private void doUpdateEndpoint(HttpServletRequest request) throws IOException
    {
        String eid = request.getParameter("id");
        Properties props = endpoints.get(eid);
        
        String addr = request.getParameter("address");
        String descr = request.getParameter("description");
        
        if (!addr.equals(props.getProperty(ADDRESS_PROP))) {
            props.setProperty(ADDRESS_PROP, addr.trim());
            props.setProperty(SUMMARY_PROP, NO_SUMMARY_VAL);
            props.setProperty(SUMMARY_PROGRESS_PROP, "0");
        }
        props.setProperty("description", descr == null ? "" : descr);
        endpoints.set(eid, props);
    }
    
    private void doToggleEndpoint(HttpServletRequest request) throws IOException
    {
        String eid = request.getParameter("id");
        Properties props = endpoints.get(eid);
        props.setProperty("enable", "" + !"true".equals(props.getProperty("enable")));
        endpoints.set(eid, props);
    }
    
    private void doRemoveEndpoint(HttpServletRequest reqest) throws IOException
    {
        String eid = reqest.getParameter("id");
        endpoints.remove(eid);
    }
    
    private synchronized void doBuildEndpointSummary(HttpServletRequest request) throws IOException
    {
        String eid = request.getParameter("id");
        //Properties props = endpoints.get(eid);
        
        SummaryBuilder.startBuilder(eid, endpoints);
        
        //if (!props.getProperty(SUMMARY_PROP).equals(BUILDING_SUMMARY_VAL)) {
            
            //props.setProperty(SUMMARY_PROP, BUILDING_SUMMARY_VAL);
            //props.setProperty(SUMMARY_PROGRESS_PROP, "0");
        //}
        
        //endpoints.set(eid, props);
    }
    
    private synchronized void doStopBuildEndpointSummary(HttpServletRequest request) throws IOException
    {
        String eid = request.getParameter("id");
        SummaryBuilder.stopBuilder(eid);
//        Properties props = endpoints.get(eid);
//        
//        if (props.getProperty(SUMMARY_PROP).equals(BUILDING_SUMMARY_VAL)) {
//            props.setProperty(SUMMARY_PROP, NO_SUMMARY_VAL);
//            props.setProperty(SUMMARY_PROGRESS_PROP, "0");
//        }
//        
//        endpoints.set(eid, props);
    }
    
    private void doGetError(HttpServletRequest request, HttpServletResponse response) throws IOException
    {
        String eid = request.getParameter("id");
        String errstr = endpoints.getError(eid);

        response.setContentType("text/plain");
        response.setCharacterEncoding("UTF-8");
        response.getWriter().println(errstr == null ? "error: an error file is not found" : errstr);
    }
    
    private void doGetSummary(HttpServletRequest request, HttpServletResponse response) throws IOException
    {
        String eid = request.getParameter("id");
        String summary = endpoints.getSummary(eid);
        
        response.setContentType("text/turtle");
        response.setCharacterEncoding("UTF-8");
        response.getWriter().println(summary == null ? "error: a summary file is not found" : summary);
    }
    
    private String listEndpoints() throws FileNotFoundException, IOException {
        File folder = new File(Constants.endpointsPath());
        
        Map<String, Properties> result = new HashMap<String, Properties>();
        
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) continue;
            
            Properties prop = endpoints.cget(fileEntry.getName());
            
            result.put(fileEntry.getName(), prop);
        }
        
        return new Gson().toJson(result);
    }
}