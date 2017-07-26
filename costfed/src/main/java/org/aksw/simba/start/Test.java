package org.aksw.simba.start;

import java.util.Arrays;
import java.util.List;

import javax.swing.text.Utilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;

import com.fluidops.fedx.DefaultEndpointListProvider;
import com.fluidops.fedx.FedXFactory;
import com.fluidops.fedx.Util;
import com.fluidops.fedx.sail.FedXSailRepository;

public class Test {
    static Logger log = LoggerFactory.getLogger(Test.class);
    
    public static void main(String[] args) throws Exception {
    
        QueryProvider qp;
        qp = new QueryProvider("../queries/");
        
        String host = "localhost";
        
        List<String> endpointsMin = Arrays.asList(
                "http://" + host + ":8890/sparql",
                "http://" + host + ":8891/sparql",
                "http://" + host + ":8892/sparql",
                "http://" + host + ":8893/sparql",
                "http://" + host + ":8894/sparql",
                "http://" + host + ":8895/sparql",
                "http://" + host + ":8896/sparql",
                "http://" + host + ":8897/sparql",
                "http://" + host + ":8898/sparql"
        );
        
        //FedXSailRepository rep = FedXFactory.initializeFederation("costfed.props", new DefaultEndpointListProvider(endpointsMin));
        FedXSailRepository rep = FedXFactory.initializeFederation("costfed.props");
        try {
            RepositoryConnection conn = rep.getConnection();
            
            String curQuery = qp.getQuery("S1");
            TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, curQuery); 
            long startTime = System.currentTimeMillis();
            TupleQueryResult res = query.evaluate();
            long count = 0;
            while (res.hasNext()) {
                BindingSet row = res.next();
                count++;
            }
            log.info("RESULT: " + count);
            while (true) {
                Thread.sleep(1000);
                log.info("tick");
            }
        } catch (Exception e) {
            
            log.error("", e);
            e.printStackTrace();
        } finally {
            rep.shutDown();
        }
    }
}
