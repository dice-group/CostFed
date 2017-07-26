package org.aksw.simba.quetsal.util;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.aksw.simba.quetsal.datastructues.Pair;
import org.aksw.simba.quetsal.datastructues.Trie2;
import org.aksw.simba.quetsal.datastructues.Tuple2;
import org.aksw.simba.quetsal.datastructues.Tuple3;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.rdf4j.http.client.util.HttpClientBuilders;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;

public class SummaryGenerator {
    static Logger log = LoggerFactory.getLogger(SummaryGenerator.class);
    
    public static interface ProgressListener
    {
        void setProgress(int val);
        void setProgressMax(int val);
        void incrementProgress();
    }
    
    // Global config
    static int MIN_TOP_OBJECTS = 10;
    static int MAX_TOP_OBJECTS = 100;
    
    // HTTP client setup
    public static HttpClientBuilder httpClientBuilder;
    public static HttpClient httpClient;
    
    static {
        httpClientBuilder = HttpClientBuilders.getSSLTrustAllHttpClientBuilder();
        httpClientBuilder.setMaxConnTotal(10);
        httpClientBuilder.setMaxConnPerRoute(5);
        //httpClientBuilder.setConnectionReuseStrategy(new NoConnectionReuseStrategy());
        httpClient = httpClientBuilder.build();
    }
    
    // repository management
    static Map<String, SPARQLRepository> reps = new HashMap<String, SPARQLRepository>();
    static synchronized SPARQLRepository createSPARQLRepository(String url) {
        SPARQLRepository repo = reps.get(url);
        if (repo == null) {
            repo = new SPARQLRepository(url);
            repo.setHttpClient(httpClient);
            repo.initialize();
            reps.put(url, repo);
        }
        return repo;
    }
    
    // utility
    static Pair<String, String> splitIRI(String iri) {
        String[] objPrts = iri.split("/");
        String objAuth = objPrts[0] + "//" + objPrts[2];
        return new Pair<String, String>(objAuth, iri.substring(objAuth.length()));
    }
    
    static boolean putIRI(String iri, long hits, Trie2.Node nd) {
        Pair<String, String> pair = splitIRI(iri);
        return Trie2.insertWord(nd, pair.getFirst(), pair.getSecond(), hits);
    }
    
    static boolean verifyIRI(Value obj) {
        if (!(obj instanceof IRI)) return false;
        return verifyIRI(obj.stringValue());
    }
    
    static boolean verifyIRI(String sval) {
        try {
            // exclude virtuoso specific data
            if (sval.startsWith("http://www.openlinksw.com/schemas") || sval.startsWith("http://www.openlinksw.com/virtrd")) return false;
            URL url = new URL(sval);
            new URI(url.getProtocol(), url.getAuthority(), url.getPath(), url.getQuery(), url.getRef()).toURL().toString();
            return url.getProtocol() != null && url.getAuthority() != null && !url.getAuthority().isEmpty();
        } catch (Exception e) {
            //log.warn("skip IRI: " + sval + ", error: " + e.getMessage());
            return false;
        }
    }
    
    static String makeWellFormed(String uri) {
        try {
            URL url = new URL(uri);
            return new URI(url.getProtocol(), url.getAuthority(), url.getPath(), url.getQuery(), url.getRef()).toURL().toString();
        } catch (Exception e) {
            log.error("Error in " + uri);
            throw new RuntimeException(e);
        }
    }
    
    static String encodeStringLiteral(String s) {
        if (s.indexOf((int)'"') == -1) {
            return s;
        }
        return s.replace("\"", "\\\"");
    }
    
    // Generator methods
    public static String generateSummary(String endpoint, String graph, int branchLimit, TaskManager tm, ProgressListener pl) throws Exception
    {
        List<String> lstPredRaw = getPredicates(endpoint, graph);
        log.info("total distinct predicates: "+ lstPredRaw.size() + " for endpoint: " + endpoint);
        // filter predicates
        List<String> lstPred = new ArrayList<String>();
        for (int i = 0; i < lstPredRaw.size(); i++)
        {
            final String predicate = lstPredRaw.get(i);
            if (!verifyIRI(predicate)) continue;
            lstPred.add(predicate);
        }
        log.info("total filtered distinct predicates: "+ lstPred.size() + " for endpoint: " + endpoint);
        pl.setProgressMax(lstPred.size() + 3);
        pl.incrementProgress();
        
        long totalSbj = getDistinctSubjectCount(endpoint);
        log.info("total distinct subjects: "+ totalSbj + " for endpoint: " + endpoint);
        pl.incrementProgress();
        
        long totalObj = getDistinctObjectCount(endpoint);
        log.info("total distinct objects: "+ totalObj + " for endpoint: " + endpoint);
        pl.incrementProgress();
        
        final StringBuilder sb = new StringBuilder();
        sb.append("@prefix ds:<http://aksw.org/quetsal/> .\n");
        sb.append("[] a ds:Service ;\n");
        sb.append("     ds:url   <"+endpoint+"> ;\n");
        
        List<Future<Void>> subtasks = new ArrayList<Future<Void>>();
        
        final AtomicLong totalTrpl = new AtomicLong(0);
        for (int i = 0; i < lstPred.size(); i++)
        {
            final String predicate = lstPred.get(i);
            
            Future<Void> statfuture = tm.addTask(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    Tuple2<String, Long> stat = writePrefixes(predicate, endpoint, graph, branchLimit);
                    synchronized(sb) {
                        sb.append(stat.getValue0());
                        totalTrpl.addAndGet(stat.getValue1());
                    }
                    pl.incrementProgress();
                    return null;
                }
            });
            subtasks.add(statfuture);
        }
        
        for (int i = 0; i < lstPred.size(); i++)
        {
            subtasks.get(i).get();
        }
       

        sb.append("     ds:totalSbj "+totalSbj+" ; \n");  // this is not representing the actual number of distinct subjects in a datasets since the same subject URI can be shaared by more than one predicate. but we keep this to save time.  
        sb.append("     ds:totalObj "+totalObj+" ; \n");  
        sb.append("     ds:totalTriples "+totalTrpl+" ; \n");
        sb.append("             .\n");

        //     bw.append("     sd:totalTriples \""+totalTrpl+"\" ;");
        //bw.append("#---------End---------");
        //bw.close();
        return sb.toString();
    }
    
    /**
     * Write all the distinct subject prefixes  for triples with predicate p. 
     * @param predicate  Predicate
     * @param endpoint Endpoint URL
     * @param graph named Graph
     * @param branchLimit Branching Limit for Trie
     * @throws IOException  IO Error
     */
    public static Tuple2<String, Long> writePrefixes(String predicate, String endpoint, String graph, int branchLimit) throws IOException {
        StringBuilder sb = new StringBuilder();
        
        sb.append("     ds:capability\n");
        sb.append("         [\n");
        sb.append("           ds:predicate  <" + predicate + "> ;");
        
        long limit = 1000000;
        boolean isTypePredicate = predicate.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

        long rsCount = 0;
        long uniqueSbj = 0;
        long uniqueObj = 0;

        Trie2.Node rootSbj = Trie2.initializeTrie();
        Trie2.Node rootObj = Trie2.initializeTrie();

        SPARQLRepository repo = createSPARQLRepository(endpoint);

        long top = 0;
        
        long rc;
        do
        {
            rc = 0;
            StringBuilder qb = new StringBuilder();
            qb.append("SELECT ?s count(?o) as ?oc");
            if (null != graph) {
                qb.append(" FROM <").append(graph).append('>');
            }
            qb.append(" WHERE { ?s <").append(predicate).append("> ?o. } GROUP BY ?s LIMIT ").append(limit).append(" OFFSET ").append(top);
            
            RepositoryConnection conn = repo.getConnection();
            try {
                TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, qb.toString()); 
                TupleQueryResult res = query.evaluate();
                
                while (res.hasNext()) 
                {
                    ++rc;
                    BindingSet bs = res.next();
                    Value curSbj = bs.getValue("s");
                    ++uniqueSbj;
                    if (verifyIRI(curSbj)) {
                        putIRI(curSbj.stringValue(), Long.parseLong(bs.getValue("oc").stringValue()), rootSbj);
                    }
                }
                res.close();
            } finally {
                conn.close();
            }
            top += rc;
        } while (rc == limit);
        

        top = 0;
        do
        {
            rc = 0;
            StringBuilder qb = new StringBuilder();
            qb.append("SELECT ?o (count(?s) as ?sc)");
            if (null != graph) {
                qb.append(" FROM <").append(graph).append('>');
            }
            qb.append(" WHERE { ?s <").append(predicate).append("> ?o. } GROUP BY ?o LIMIT ").append(limit).append(" OFFSET ").append(top);
            
            RepositoryConnection conn = repo.getConnection();
            try {
                TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, qb.toString()); 
                TupleQueryResult res = query.evaluate();
                
                while (res.hasNext()) 
                {
                    ++rc;
                    BindingSet bs = res.next();
                    Value curObj = bs.getValue("o");
                    ++uniqueObj;
                    if (verifyIRI(curObj)) {
                        putIRI(curObj.stringValue(), Long.parseLong(bs.getValue("sc").stringValue()), rootObj);
                    }
                }
                res.close();
            } finally {
                conn.close();
            }
            top += rc;
        } while (rc == limit);

        
        rsCount = getTripleCount(predicate, endpoint);
        
        boolean first = true;
        sb.append("\n           ds:topSbjs");
        List<Pair<String, Long>> topsbjstotal = Trie2.findMostHittable(rootSbj, 1000 > MAX_TOP_OBJECTS ? 1000 : MAX_TOP_OBJECTS);
        List<Pair<String, Long>> topsbjs = new ArrayList<Pair<String, Long>>();
        List<String> middlesbjs = new ArrayList<String>();
        long avrmiddlesbjcard = doSaleemAlgo(MIN_TOP_OBJECTS, MAX_TOP_OBJECTS, topsbjstotal, topsbjs, middlesbjs);
        for (Pair<String, Long> p : topsbjs) {
            if (!first) sb.append(","); else first = false;
            sb.append("\n             [ ds:subject <").append(makeWellFormed(p.getFirst())).append(">; ds:card ").append(p.getSecond()).append(" ]");
        }
        if (!middlesbjs.isEmpty()) {
            sb.append(",\n             [ ds:middle ");
            first = true;
            for (String ms : middlesbjs) {
                if (!first) sb.append(","); else first = false;
                sb.append('<').append(makeWellFormed(ms)).append('>');
            }
            sb.append("; ds:card ").append(avrmiddlesbjcard).append(" ]");
        }
        if (topsbjs.isEmpty()) sb.append(" []");
        
        first = true;
        sb.append(";\n           ds:topObjs");

        List<Pair<String, Long>> topobjstotal = Trie2.findMostHittable(rootObj, isTypePredicate ? Integer.MAX_VALUE : (1000 > MAX_TOP_OBJECTS ? 1000 : MAX_TOP_OBJECTS));
        List<Pair<String, Long>> topobjs = null;
        List<String> middleobjs = new ArrayList<String>();
        long avrmiddleobjcard = 0;
        if (!isTypePredicate) {
            topobjs = new ArrayList<Pair<String, Long>>();
            avrmiddleobjcard = doSaleemAlgo(MIN_TOP_OBJECTS, MAX_TOP_OBJECTS, topobjstotal, topobjs, middleobjs);
        } else {
            topobjs = topobjstotal;
        }
        for (Pair<String, Long> p : topobjs) {
            if (!first) sb.append(","); else first = false;
            sb.append("\n             [ ds:object <").append(makeWellFormed(p.getFirst())).append(">; ds:card ").append(p.getSecond()).append(" ]");
        }
        if (!middleobjs.isEmpty()) {
            sb.append(",\n             [ ds:middle ");
            first = true;
            for (String ms : middleobjs) {
                if (!first) sb.append(","); else first = false;
                sb.append('<').append(makeWellFormed(ms)).append('>');
            }
            sb.append("; ds:card ").append(avrmiddleobjcard).append(" ]");
        }
        if (topobjs.isEmpty()) sb.append(" []");
        
        first = true;
        sb.append(";\n           ds:subjPrefixes");
        List<Tuple3<String, Long, Long>> sprefs = Trie2.gatherPrefixes(rootSbj, branchLimit);
        for (Tuple3<String, Long, Long> t : sprefs) {
            if (!first) sb.append(","); else first = false;
            sb.append("\n             [ ds:prefix \"").append(encodeStringLiteral(t.getValue0())).append("\"; ds:unique ").append(t.getValue1()).append("; ds:card ").append(t.getValue2()).append(" ]");
        }
        if (sprefs.isEmpty()) sb.append(" []");
        
        first = true;
        sb.append(";\n           ds:objPrefixes");
        if (!isTypePredicate) {
            List<Tuple3<String, Long, Long>> oprefs = Trie2.gatherPrefixes(rootObj, branchLimit);
            for (Tuple3<String, Long, Long> t : oprefs) {
                if (!first) sb.append(","); else first = false;
                sb.append("\n             [ ds:prefix \"").append(encodeStringLiteral(t.getValue0())).append("\"; ds:unique ").append(t.getValue1()).append("; ds:card ").append(t.getValue2()).append(" ]");
            }
            if (oprefs.isEmpty()) sb.append(" []");
        } else {
            sb.append(" []");
        }
        
        sb.append(";\n");
        //log.info("subject: " + topsbjs);
        //log.info("object: " + topobjs);
        
        sb.append("           ds:distinctSbjs " + uniqueSbj + " ;\n");
        //sb.append("\n           ds:avgSbjSelectivity  " + (1 / (double)distinctSbj) + " ;");
        //double distinctObj = getObj(lstPred.get(i), endpoint);
        sb.append("           ds:distinctObjs  " + uniqueObj + " ;\n");
        //sb.append("\n           ds:avgObjSelectivity  " + (1 / (double)distinctObj) + " ;\n");
        
        sb.append("           ds:triples    " + rsCount + " ;\n");
        sb.append("         ] ;\n");
        
        return new Tuple2<String, Long>(sb.toString(), rsCount);
    }
    
    /**
     * 
     * 
     * @param objects cardinality ordered list of objects
     * @param top output collection of top objects
     * @param middle collection of top objects
     * @return average cardinality
     */
    static long doSaleemAlgo(int min, int max, List<Pair<String, Long>> objects, List<Pair<String, Long>> top, List<String> middle) {
        while (!objects.isEmpty() && objects.get(objects.size() - 1).getSecond() == 1) {
            objects.remove(objects.size() - 1);
        }
            
        if (min > objects.size()) min = objects.size();
        
        for (int i = 0; i < min; ++i) {
            Pair<String, Long> obj = objects.get(i);
            top.add(obj);
        }
        
        int max2 = max < objects.size() ? max : objects.size();

        // find first diff maximum
        long maxdiff = 0;
        int maxdiffidx = -1;
        for (int i = min; i < max2 - 1; ++i) {
            long diff = objects.get(i).getSecond() - objects.get(i + 1).getSecond();
            if (diff > maxdiff) {
                maxdiff = diff;
                maxdiffidx = i;
            }
        }
        if (maxdiffidx == -1) return 0;
        // copy objects
        for (int i = min; i < maxdiffidx; ++i) {
            Pair<String, Long> obj = objects.get(i);
            top.add(obj);
        }
        
        // find next diff maximum
        maxdiff = 0;
        int nextmaxdiffidx = -1;
        for (int i = maxdiffidx + 1; i < max2 - 1; ++i) {
            long diff = objects.get(i).getSecond() - objects.get(i + 1).getSecond();
            if (diff > maxdiff) {
                maxdiff = diff;
                nextmaxdiffidx = i;
            }
        }
        if (nextmaxdiffidx == -1) {
            nextmaxdiffidx = max2;
        }
        if (maxdiffidx == nextmaxdiffidx) return 0;
        
        // copy middle objects
        long totalCard = 0;
        for (int i = maxdiffidx; i < nextmaxdiffidx; ++i) {
            Pair<String, Long> obj = objects.get(i);
            middle.add(obj.getFirst());
            totalCard += obj.getSecond();
        }
        
        return totalCard / (nextmaxdiffidx - maxdiffidx);
    }
    
    /**
     * Get total number of triple for a predicate
     * @param pred Predicate
     * @param m model
     * @return triples
     */
    public static Long getTripleCount(String pred, String endpoint) {
        String strQuery = "SELECT  (COUNT(*) AS ?triples) " + // 
                "WHERE " +
                "{" +
                "?s <"+pred+"> ?o " +
                "} " ;
        SPARQLRepository repo = createSPARQLRepository(endpoint);
        RepositoryConnection conn = repo.getConnection();
        try {
            TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, strQuery); 
            TupleQueryResult rs = query.evaluate();
            String v = rs.next().getValue("triples").stringValue();
            rs.close();
            return Long.parseLong(v);
        } finally {
            conn.close();
        }
    }
    
    public static Long getDistinctSubjectCount(String endpoint) {
        String strQuery = "SELECT  (COUNT(distinct ?s) AS ?triples) " + 
                "WHERE " +
                "{" +
                "?s ?p ?o " +
                "} " ;
        SPARQLRepository repo = createSPARQLRepository(endpoint);
        RepositoryConnection conn = repo.getConnection();
        try {
            TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, strQuery); 
            TupleQueryResult rs = query.evaluate();
            String v = rs.next().getValue("triples").stringValue();
            rs.close();
            return Long.parseLong(v);
        } finally {
            conn.close();
        }
    }
    
    public static Long getDistinctObjectCount(String endpoint) {
        String strQuery = "SELECT  (COUNT(distinct ?o) AS ?triples) " +
                "WHERE " +
                "{" +
                "?s ?p ?o " +
                "FILTER isIRI(?o)" +
                "} " ;
        SPARQLRepository repo = createSPARQLRepository(endpoint);
        RepositoryConnection conn = repo.getConnection();
        try {
            TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, strQuery); 
            TupleQueryResult rs = query.evaluate();
            String v = rs.next().getValue("triples").stringValue();
            rs.close();
            return Long.parseLong(v);
        } finally {
            conn.close();
        }
    }
    
    /**
    * Get Predicate List
    * @param endPointUrl SPARQL endPoint Url
    * @param graph Named graph
    * @return  predLst Predicates List
    */
   private static List<String> getPredicates(String endPointUrl, String graph)
   {
       String strQuery = "SELECT DISTINCT ?p";
       if (null != graph) {
           strQuery += " FROM <" + graph + ">";
       }
       strQuery += " WHERE { ?s ?p ?o }";
       
       List<String>  predLst = new ArrayList<String>();
       
       SPARQLRepository repo = createSPARQLRepository(endPointUrl);
       RepositoryConnection conn = repo.getConnection();
       try {
           TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, strQuery); 
           TupleQueryResult res = query.evaluate();
           while (res.hasNext()) 
           {
               String pred = res.next().getValue("p").toString();
               predLst.add(pred);          
           }
           res.close();
       } finally {
           conn.close();
       }
       return predLst;
   }
}
