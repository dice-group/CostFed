package com.fluidops.fedx;

import java.util.List;

import com.fluidops.fedx.structures.Endpoint;

public interface EndpointListProvider {
    List<Endpoint> getEndpoints(FedX federation);
    void close();
}
