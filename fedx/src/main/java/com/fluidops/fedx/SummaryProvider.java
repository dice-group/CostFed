package com.fluidops.fedx;

public interface SummaryProvider {
    Summary getSummary(FedX federation);
    void close();
}
