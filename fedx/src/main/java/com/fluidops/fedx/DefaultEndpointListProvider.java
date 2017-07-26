package com.fluidops.fedx;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.util.EndpointFactory;

public class DefaultEndpointListProvider implements EndpointListProvider {
    Collection<String> endpoints;
    
    public DefaultEndpointListProvider(List<String> endpoints) {
        this.endpoints = endpoints;
    }
    
   
    @Override
    public List<Endpoint> getEndpoints(FedX federation) {
        List<Endpoint> result = new ArrayList<Endpoint>();
        
        for (String url : endpoints) {
            result.add(EndpointFactory.loadSPARQLEndpoint(federation.getConfig(), federation.getHttpClient(), url));
        }
        return result;
    }
    
    /**
     * Remove a member from the federation (internal)
     * 
     * @param endpoint
     * @return
     */
    public boolean removeMember(Endpoint endpoint) {
        return endpoints.remove(endpoint);
    }


    @Override
    public void close() {
        
    }   
}
