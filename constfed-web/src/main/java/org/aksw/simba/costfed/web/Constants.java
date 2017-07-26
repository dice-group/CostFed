package org.aksw.simba.costfed.web;

import java.io.File;

public class Constants {
    static final String ENDPOINTS_PATH = "endpoints";
    static final String ERRORS_PATH = "errors";
    static final String SUMMARIES_PATH = "summaries";
    
    static final String endpointsPath() {
        return System.getProperty("costfed.dir") + File.separatorChar + Constants.ENDPOINTS_PATH;
    }
    
    static final String errorsPath() {
        return System.getProperty("costfed.dir") + File.separatorChar + Constants.ERRORS_PATH;
    }
    
    static final String summariesPath() {
        return System.getProperty("costfed.dir") + File.separatorChar + Constants.SUMMARIES_PATH;
    }
}
