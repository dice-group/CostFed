package org.aksw.simba.start;

import java.util.Arrays;
import java.util.List;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fluidops.fedx.DefaultEndpointListProvider;
import com.fluidops.fedx.FedXFactory;
import com.fluidops.fedx.sail.FedXSailRepository;

public class Test {
    static Logger log = LoggerFactory.getLogger(Test.class);
    
    public static void main(String[] args) throws Exception {
    
        //QueryProvider qp;
        //qp = new QueryProvider("../queries/");
        
        String host = "localhost";
        
        List<String> endpointsMin = Arrays.asList(
                "http://" + host + ":8890/sparql",
                "http://" + host + ":8891/sparql",
                "http://" + host + ":8892/sparql"
                /*
                , "http://" + host + ":8893/sparql",
                "http://" + host + ":8894/sparql",
                "http://" + host + ":8895/sparql",
                "http://" + host + ":8896/sparql",
                "http://" + host + ":8897/sparql",
                "http://" + host + ":8898/sparql"
                */
        );
        
        FedXSailRepository rep = FedXFactory.initializeFederation("costfed.props", new DefaultEndpointListProvider(endpointsMin));
        //FedXSailRepository rep = FedXFactory.initializeFederation("costfed.props");
        try {
            RepositoryConnection conn = rep.getConnection();
            String testQuery = " SELECT  ?s ?Concept WHERE   { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?Concept } ";
            String query0 = "PREFIX  owl:  <http://www.w3.org/2002/07/owl#>\r\n" + 
                    "PREFIX  rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\r\n" + 
                    "PREFIX  foaf: <http://xmlns.com/foaf/0.1/>\r\n" + 
                    "\r\n" + 
                    "SELECT DISTINCT  ?a ?v0 WHERE   {\r\n" + 
                    "    ?a rdf:type foaf:Person .\r\n" + 
                    "    ?a foaf:name ?v0\r\n" + 
                    "}\r\n"; 
                    //"OFFSET  10000\r\n" + 
                    //"LIMIT   1000 ";
            
            String curQuery = testQuery;
                    //qp.getQuery("S1");
            TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, curQuery); 
            long startTime = System.currentTimeMillis();
            TupleQueryResult res = query.evaluate();
            long count = 0;
            while (res.hasNext()) {
                BindingSet row = res.next();
                count++;
            }
            log.info("RESULT: " + count);
            //while (true) {
            //    Thread.sleep(1000);
            //    log.info("tick");
            //}
        } catch (Exception e) {
            
            log.error("", e);
            e.printStackTrace();
        } finally {
            rep.shutDown();
        }
    }
}
