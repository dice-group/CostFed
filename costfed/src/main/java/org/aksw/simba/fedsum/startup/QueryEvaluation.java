package org.aksw.simba.fedsum.startup;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.List;

import org.aksw.simba.fedsum.FedSumConfig;
import org.aksw.sparql.query.algebra.helpers.BGPGroupGenerator;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.repository.sail.SailRepository;

import com.fluidops.fedx.Config;
import com.fluidops.fedx.FedXFactory;

public class QueryEvaluation<repo> {
	
	/**
	 * Basic graph pattern (bgp) groups
	 * @param args
	 * @throws Exception 
	 */
	public static List<List<StatementPattern>> theDNFgrps;
	/**
	 * write results into file
	 */
	public static BufferedWriter bw ;
	
	public static void main(String[] args) throws Exception 
	{
	long strtTime = System.currentTimeMillis();
	String FedSummaries = "summaries\\FedSumFedBench.n3";
	
	Config config = new Config(args[0]);
	String mode = "ASK_dominant";  //{ASK_dominant, Index_dominant}
	double commonPredThreshold = 0.33 ;  //considered a predicate as common predicate if it is presenet in 33% available data sources

	FedSumConfig.initialize(FedSummaries, mode, commonPredThreshold);  // must call this function only one time at the start to load configuration information. Please specify the FedSum mode. 
	System.out.println("One time configuration loading time : "+ (System.currentTimeMillis()-strtTime));

    SailRepository repo = FedXFactory.initializeSparqlFederation(config, FedSumConfig.dataSources);

	String cd1 = "SELECT ?predicate ?object WHERE { " +     //cd1
			"{ <http://dbpedia.org/resource/Barack_Obama> ?predicate ?object }" +
			" UNION " +
			" { ?subject <http://www.w3.org/2002/07/owl#sameAs> <http://dbpedia.org/resource/Barack_Obama> ." +
			"?subject ?predicate ?object }  " +
			"}";
	String cd2 = "SELECT ?party ?page  WHERE { " +   //cd2
		  " <http://dbpedia.org/resource/Barack_Obama> <http://dbpedia.org/ontology/party> ?party ." +
		 " ?x <http://data.nytimes.com/elements/topicPage> ?page ." +
			"   ?x <http://www.w3.org/2002/07/owl#sameAs> <http://dbpedia.org/resource/Barack_Obama> ."+
			"}";
	String cd3 = "SELECT ?president ?party ?page WHERE { " + //cd3
   "?president <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/ontology/President> ." +
   "?president <http://dbpedia.org/ontology/nationality> <http://dbpedia.org/resource/United_States> ." +
   "?president <http://dbpedia.org/ontology/party> ?party ." +
   "?x <http://data.nytimes.com/elements/topicPage> ?page ." +
   "?x <http://www.w3.org/2002/07/owl#sameAs> ?president ." +
"}";
	String cd4 = "SELECT ?actor ?news WHERE {"+   //cd4
		   "?film <http://purl.org/dc/terms/title> 'Tarzan' ."+
			"   ?film <http://data.linkedmdb.org/resource/movie/actor> ?actor ."+
			 "  ?actor <http://www.w3.org/2002/07/owl#sameAs> ?x."+
			  " ?y <http://www.w3.org/2002/07/owl#sameAs> ?x ."+
			  " ?y <http://data.nytimes.com/elements/topicPage> ?news"+
			"}";
	String cd5 = "SELECT ?film ?director ?genre WHERE {"+    //cd5 
		   "?film <http://dbpedia.org/ontology/director>  ?director ."+
			"   ?director <http://dbpedia.org/ontology/nationality> <http://dbpedia.org/resource/Italy> ."+
			 "  ?x <http://www.w3.org/2002/07/owl#sameAs> ?film ."+
			  " ?x <http://data.linkedmdb.org/resource/movie/genre> ?genre ."+
			"}";
	String cd6 = "SELECT ?name ?location ?news WHERE {"+ //cd 6
		   "?artist <http://xmlns.com/foaf/0.1/name> ?name ."+
			"   ?artist <http://xmlns.com/foaf/0.1/based_near> ?location ."+
			 "  ?location <http://www.geonames.org/ontology#parentFeature> ?germany . "+
			  " ?germany <http://www.geonames.org/ontology#name> 'Federal Republic of Germany'"+
			"}";
	String cd7= "SELECT ?location ?news WHERE {"+ //7
		   "?location <http://www.geonames.org/ontology#parentFeature> ?parent ."+ 
			"   ?parent <http://www.geonames.org/ontology#name> 'California' ."+
			 "  ?y <http://www.w3.org/2002/07/owl#sameAs> ?location ."+
			  " ?y <http://data.nytimes.com/elements/topicPage> ?news "+
			"}";
	//-----------------------------------------LS-----------------------------------
	String S1 = "SELECT ?predicate ?object  \n" //cd1
			+ "WHERE \n"
			+ " { \n" +    
			"     { \n"
			+ "     <http://dbpedia.org/resource/Barack_Obama> ?predicate ?object \n"
			+ "     }\n" +
			" UNION \n " +
			" { \n"
			+ "   ?subject <http://www.w3.org/2002/07/owl#sameAs> <http://dbpedia.org/resource/Barack_Obama> .\n" +
			"   ?subject ?predicate ?object \n"
			+ "  } \n " +
			"}";
	
	String ls1 = "SELECT ?drug ?melt WHERE {"+
	    "{ ?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/meltingPoint> ?melt. }"+
	    "    UNION"+
	    "    { ?drug <http://dbpedia.org/ontology/Drug/meltingPoint> ?melt . }"+
	    "}";
	String ls2 = "SELECT ?predicate ?object WHERE {"+
	    "{ <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB00201> ?predicate ?object . }"+
	    "UNION    "+
	    "{ <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB00201> <http://www.w3.org/2002/07/owl#sameAs> ?caff ."+
	    "  ?caff ?predicate ?object . } "+
	"}";
	String ls3 = "SELECT ?Drug ?IntDrug ?IntEffect WHERE { "+
	   " ?Drug <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/ontology/Drug> ."+
	   " ?y <http://www.w3.org/2002/07/owl#sameAs> ?Drug ."+
	   " ?Int <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/interactionDrug1> ?y ."+
	   " ?Int <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/interactionDrug2> ?IntDrug ."+
	   " ?Int <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/text> ?IntEffect . "+
	"}";
	
	String ls4 = "SELECT ?drugDesc ?cpd ?equation WHERE {"+
		   "?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/drugCategory> <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugcategory/cathartics> ."+
			"   ?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/keggCompoundId> ?cpd ."+
			 "  ?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/description> ?drugDesc ."+
			  " ?enzyme <http://bio2rdf.org/ns/kegg#xSubstrate> ?cpd ."+
			   "?enzyme <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://bio2rdf.org/ns/kegg#Enzyme> ."+
			   "?reaction <http://bio2rdf.org/ns/kegg#xEnzyme> ?enzyme ."+
			   "?reaction <http://bio2rdf.org/ns/kegg#equation> ?equation . "+
			"}";
String ls5 = "SELECT $drug $keggUrl $chebiImage WHERE {"+
		  "$drug <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/drugs> ."+
			"  $drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/keggCompoundId> $keggDrug ."+
			"  $keggDrug <http://bio2rdf.org/ns/bio2rdf#url> $keggUrl ."+
			 " $drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/genericName> $drugBankName ."+
			 " $chebiDrug <http://purl.org/dc/elements/1.1/title> $drugBankName ."+
			 " $chebiDrug <http://bio2rdf.org/ns/bio2rdf#image> $chebiImage ."+
			"}" ;
	String ls6 = "SELECT ?drug ?title WHERE { "+
		 "?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/drugCategory> <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugcategory/micronutrient> ."+
			" ?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/casRegistryNumber> ?id ."+
			" ?keggDrug <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://bio2rdf.org/ns/kegg#Drug> ."+
			" ?keggDrug <http://bio2rdf.org/ns/bio2rdf#xRef> ?id ."+
			" ?keggDrug <http://purl.org/dc/elements/1.1/title> ?title ."+
		"}";
	String ls7 = "SELECT $drug $transform $mass WHERE {  "+
	 	"{ $drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/affectedOrganism>  'Humans and other mammals'."+
	 	" 	  $drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/casRegistryNumber> $cas ."+
	 	 "	  $keggDrug <http://bio2rdf.org/ns/bio2rdf#xRef> $cas ."+
	 	 "	  $keggDrug <http://bio2rdf.org/ns/bio2rdf#mass> $mass"+
	 	 "	      FILTER ( $mass > '5' )"+
	 	 "	} "+
	 	 "	  OPTIONAL { $drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/biotransformation> $transform . } "+
	 	"}";
	//------------------------------------ld-----------------------------------
	String ld1 = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"+
		"SELECT * WHERE {"+
		"?paper <http://data.semanticweb.org/ns/swc/ontology#isPartOf> <http://data.semanticweb.org/conference/iswc/2008/poster_demo_proceedings> ."+
		"?paper <http://swrc.ontoware.org/ontology#author> ?p ."+
		"?p rdfs:label ?n ."+
		"}";
	
	String ld2 = "SELECT * WHERE {" +
			"?proceedings <http://data.semanticweb.org/ns/swc/ontology#relatedToEvent>  <http://data.semanticweb.org/conference/eswc/2010> ." +
			"?paper <http://data.semanticweb.org/ns/swc/ontology#isPartOf> ?proceedings ." +
			"?paper <http://swrc.ontoware.org/ontology#author> ?p ." +
			"}";
	
	String ld3 = "PREFIX owl: <http://www.w3.org/2002/07/owl#>" +
			"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>" +
			"SELECT * WHERE {" +
			"?paper <http://data.semanticweb.org/ns/swc/ontology#isPartOf> <http://data.semanticweb.org/conference/iswc/2008/poster_demo_proceedings> ." +
			"?paper <http://swrc.ontoware.org/ontology#author> ?p ." +
			"?p owl:sameAs ?x ." +
			"?p rdfs:label ?n ." +
			"}";
	
	String ld4 = "SELECT * WHERE {" +
			"?role <http://data.semanticweb.org/ns/swc/ontology#isRoleAt> <http://data.semanticweb.org/conference/eswc/2010> ." +
			"?role <http://data.semanticweb.org/ns/swc/ontology#heldBy> ?p ." +
			"?paper <http://swrc.ontoware.org/ontology#author> ?p ." +
			"?paper <http://data.semanticweb.org/ns/swc/ontology#isPartOf> ?proceedings ." +
			"?proceedings <http://data.semanticweb.org/ns/swc/ontology#relatedToEvent>  <http://data.semanticweb.org/conference/eswc/2010> ." +
			"}";
	
	String ld5 = "PREFIX dbpedia: <http://dbpedia.org/resource/>" +
			"PREFIX dbprop: <http://dbpedia.org/property/>" +
			"PREFIX dbowl: <http://dbpedia.org/ontology/>" +
			"PREFIX foaf: <http://xmlns.com/foaf/0.1/>" +
			"PREFIX owl: <http://www.w3.org/2002/07/owl#>" +
			"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
			"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>" +
			"PREFIX skos: <http://www.w3.org/2004/02/skos/core#>" +
			"PREFIX factbook: <http://www4.wiwiss.fu-berlin.de/factbook/ns#>" +
			"PREFIX mo: <http://purl.org/ontology/mo/>" +
			"PREFIX dc: <http://purl.org/dc/elements/1.1/>" +
			"PREFIX fb: <http://rdf.freebase.com/ns/>" +
			"SELECT * WHERE {" +
			"?a dbowl:artist dbpedia:Michael_Jackson ." +
			"?a rdf:type dbowl:Album ." +
			"?a foaf:name ?n ." +
			"}";
	
	String ld6 = "PREFIX dbpedia: <http://dbpedia.org/resource/>" +
			"PREFIX dbowl: <http://dbpedia.org/ontology/>" +
			"PREFIX owl: <http://www.w3.org/2002/07/owl#>" +
			"PREFIX linkedMDB: <http://data.linkedmdb.org/resource/>" +
			"PREFIX foaf: <http://xmlns.com/foaf/0.1/>" +
			"SELECT * WHERE {" +
			"?director dbowl:nationality dbpedia:Italy ." +
			"?film dbowl:director ?director." +
			"?x owl:sameAs ?film ." +
			"?x foaf:based_near ?y ." +
			"?y <http://www.geonames.org/ontology#officialName> ?n ." +
			"}";
	
	String ld7 = "PREFIX dbpedia: <http://dbpedia.org/resource/>" +
			"PREFIX dbowl: <http://dbpedia.org/ontology/>" +
			"PREFIX owl: <http://www.w3.org/2002/07/owl#>" +
			"PREFIX linkedMDB: <http://data.linkedmdb.org/resource/>" +
			"PREFIX foaf: <http://xmlns.com/foaf/0.1/>" +
			"PREFIX gn: <http://www.geonames.org/ontology#>" +
			"SELECT * WHERE {" +
			"?x gn:parentFeature <http://sws.geonames.org/2921044/> ." +
			"?x gn:name ?n ." +
			"}";
	
	String ld8 = "PREFIX kegg: <http://bio2rdf.org/ns/kegg#>" +
			"PREFIX drugbank: <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/>" +
			"PREFIX owl: <http://www.w3.org/2002/07/owl#>" +
			"PREFIX foaf: <http://xmlns.com/foaf/0.1/>" +
			"PREFIX skos: <http://www.w3.org/2004/02/skos/core#>" +
			"SELECT * WHERE {" +
			"?drug drugbank:drugCategory <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugcategory/micronutrient> ." +
			"?drug drugbank:casRegistryNumber ?id ." +
			"?drug owl:sameAs ?s ." +
			"?s foaf:name ?o ." +
			"?s skos:subject ?sub ." +
			"}";
	
	String ld9 = "PREFIX geo-ont: <http://www.geonames.org/ontology#>" +
			"PREFIX dbpedia: <http://dbpedia.org/resource/>" +
			"PREFIX dbprop: <http://dbpedia.org/property/>" +
			"PREFIX dbowl: <http://dbpedia.org/ontology/>" +
			"PREFIX foaf: <http://xmlns.com/foaf/0.1/>" +
			"PREFIX owl: <http://www.w3.org/2002/07/owl#>" +
			"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
			"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>" +
			"PREFIX skos: <http://www.w3.org/2004/02/skos/core#>" +
			"PREFIX factbook: <http://www4.wiwiss.fu-berlin.de/factbook/ns#>" +
			"PREFIX mo: <http://purl.org/ontology/mo/>" +
			"PREFIX dc: <http://purl.org/dc/elements/1.1/>" +
			"SELECT * WHERE {" +
			"?x skos:subject <http://dbpedia.org/resource/Category:FIFA_World_Cup-winning_countries> ." +
			"?p dbowl:managerClub ?x ." +
			"?p foaf:name \"Luiz Felipe Scolari\" @en." +
					"}";
	
	String ld10 = "PREFIX dbpedia: <http://dbpedia.org/resource/>" +
			"PREFIX dbprop: <http://dbpedia.org/property/>" +
			"PREFIX dbowl: <http://dbpedia.org/ontology/>" +
			"PREFIX foaf: <http://xmlns.com/foaf/0.1/>" +
			"PREFIX owl: <http://www.w3.org/2002/07/owl#>" +
			"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
			"PREFIX skos: <http://www.w3.org/2004/02/skos/core#>" +
			"PREFIX factbook: <http://www4.wiwiss.fu-berlin.de/factbook/ns#>" +
			"SELECT * WHERE {" +
			"?n skos:subject <http://dbpedia.org/resource/Category:Chancellors_of_Germany> ." +
			"?n owl:sameAs ?p2 ." +
			"?p2 <http://data.nytimes.com/elements/latest_use> ?u ." +
			"}";
	
	String ld11 = "PREFIX geo-ont: <http://www.geonames.org/ontology#>" +
			"PREFIX dbpedia: <http://dbpedia.org/resource/>" +
			"PREFIX dbprop: <http://dbpedia.org/property/>" +
			"PREFIX dbowl: <http://dbpedia.org/ontology/>" +
		"PREFIX foaf: <http://xmlns.com/foaf/0.1/>" +
			"PREFIX owl: <http://www.w3.org/2002/07/owl#>" +
			"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
			"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>" +
			"PREFIX skos: <http://www.w3.org/2004/02/skos/core#>" +
			"PREFIX factbook: <http://www4.wiwiss.fu-berlin.de/factbook/ns#>" +
			"PREFIX mo: <http://purl.org/ontology/mo/>" +
			"PREFIX dc: <http://purl.org/dc/elements/1.1/>" +
			"SELECT * WHERE {" +
			"?x dbowl:team dbpedia:Eintracht_Frankfurt ." +
			"?x rdfs:label ?y ." +
			"?x dbowl:birthDate ?d ." +
			"?x dbowl:birthPlace ?p ." +
			"?p rdfs:label ?l ." +
			"} ";
	String sp2b1 = "SELECT  ?yr"+
			"WHERE {"+
		  "?journal <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://localhost/vocabulary/bench/Journal> ."+
		  "?journal <http://purl.org/dc/elements/1.1/title> \"Journal 1 (1940)\"^^<http://www.w3.org/2001/XMLSchema#string> ."+
		  "?journal <http://purl.org/dc/terms/issued> ?yr "+
		"}";
	String sp2b2 = "SELECT ?inproc ?author ?booktitle ?title "+
		       "?proc ?ee ?page ?url ?yr ?abstract"+
		    	"	   WHERE {"+
		    	"	     ?inproc <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://localhost/vocabulary/bench/Inproceedings> ."+
		    	"	     ?inproc <http://purl.org/dc/elements/1.1/creator> ?author ."+
		    	"	     ?inproc <http://localhost/vocabulary/bench/booktitle> ?booktitle ."+
		    	"	     ?inproc <http://purl.org/dc/elements/1.1/title> ?title ."+
		    	"	     ?inproc <http://purl.org/dc/terms/partOf> ?proc ."+
		    	"	     ?inproc <http://www.w3.org/2000/01/rdf-schema#seeAlso> ?ee ."+
		    	"	     ?inproc <http://swrc.ontoware.org/ontology#pages> ?page ."+
		    	"	     ?inproc <http://xmlns.com/foaf/0.1/homepage> ?url ."+
		    	"	     ?inproc <http://purl.org/dc/terms/issued> ?yr"+ 
		    	"	     OPTIONAL {"+
		    	"	       ?inproc <http://localhost/vocabulary/bench/abstract> ?abstract"+
		    	"	     }"+
		    	"	   }"+
		    	"	   ORDER BY ?yr";
	
	String sp2b4 = "SELECT DISTINCT ?name1 ?name2 " +
			"			WHERE {" +
			"		  ?article1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://localhost/vocabulary/bench/Article> ." +
			"		  ?article2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://localhost/vocabulary/bench/Article> ." +
			"		  ?article1 <http://purl.org/dc/elements/1.1/creator> ?author1 ." +
			"		  ?author1 <http://xmlns.com/foaf/0.1/name> ?name1 ." +
			"		  ?article2 <http://purl.org/dc/elements/1.1/creator> ?author2 ." +
			"		  ?author2 <http://xmlns.com/foaf/0.1/name> ?name2 ." +
			"		  ?article1 <http://swrc.ontoware.org/ontology#journal> ?journal ." +
			"		  ?article2 <http://swrc.ontoware.org/ontology#journal> ?journal" +
			"		  FILTER (?name1<?name2)" +
			"		}" ;
		
	String sp2b8 = "SELECT DISTINCT ?name" +
			"			WHERE {" +
			"		  ?erdoes <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> ." +
			"		  ?erdoes <http://xmlns.com/foaf/0.1/name> \"Paul Erdoes\"^^<http://www.w3.org/2001/XMLSchema#string> ." +
					"		  {" +
					"		    ?document <http://purl.org/dc/elements/1.1/creator> ?erdoes ." +
					"		    ?document <http://purl.org/dc/elements/1.1/creator> ?author ." +
					"		    ?document2 <http://purl.org/dc/elements/1.1/creator> ?author ." +
					"		    ?document2 <http://purl.org/dc/elements/1.1/creator> ?author2 ." +
					"		    ?author2 <http://xmlns.com/foaf/0.1/name> ?name" +
					"		    FILTER (?author!=?erdoes &&" +
					"		            ?document2!=?document &&" +
					"	            ?author2!=?erdoes &&" +
					"		            ?author2!=?author)" +
					"		  } UNION {" +
					"		    ?document <http://purl.org/dc/elements/1.1/creator> ?erdoes." +
					"		    ?document <http://purl.org/dc/elements/1.1/creator> ?author." +
					"		    ?author <http://xmlns.com/foaf/0.1/name> ?name" +
					"		    FILTER (?author!=?erdoes)" +
					"		  }" +
					"		}" ;
	String sp2b10 = "SELECT ?subject ?predicate" +
			"WHERE {" +
			"  ?subject ?predicate <http://localhost/persons/Paul_Erdoes>" +
			"}";
	
	String sp2b11 = "SELECT ?ee" +
			"			WHERE {" +
			"		  ?publication <http://www.w3.org/2000/01/rdf-schema#seeAlso> ?ee" +
			"		}" +
			"		ORDER BY ?ee" +
			"" +
			"		LIMIT 10" +
			"		OFFSET 50" ;
	String fedSumQry = "PREFIX  cp: <http://common/schema/>"
			+ "PREFIX  ns1_3: <http://auth13/schema/>"
			+ "PREFIX  ns3:<http://auth3/schema/>"
			+ "SELECT *  WHERE"
			+ " {"
			+ "			ns3:s3  cp:p9    ?v0."
			+ "					?s1        cp:p0    ?v0."
			+ "					?s1        cp:p1    ?v1 . "
			+ "					?v1        cp:p2    ?v2 . "
			+ "					?v1     cp:p3   \"o35\".} "
			+ "";
	String C10 ="PREFIX tcga: <http://tcga.deri.ie/schema/> \n"
			+ "PREFIX kegg: <http://bio2rdf.org/ns/kegg#>\n"
			+ "PREFIX dbpedia: <http://dbpedia.org/ontology/>\n"
+ "PREFIX drugbank: <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/>\n"
+ "PREFIX purl: <http://purl.org/dc/terms/>\n"
+ "SELECT  DISTINCT ?patient  ?gender ?country ?popDensity ?drugName ?indication ?formula ?compound \n"
+ "WHERE\n"
+ "{\n"
+ "?uri tcga:bcr_patient_barcode ?patient .\n"
+ "?patient tcga:gender ?gender.\n"
+ "?patient dbpedia:country ?country.\n"
+ "?country dbpedia:populationDensity ?popDensity.\n"
+ "?patient tcga:bcr_drug_barcode ?drugbcr.\n"
+ "?drugbcr tcga:drug_name ?drugName. \n"
+ "?drgBnkDrg  drugbank:genericName ?drugName.\n"
+ "?drgBnkDrg  drugbank:indication ?indication.\n"
+ "?drgBnkDrg  drugbank:chemicalFormula ?formula.\n"
+ "?drgBnkDrg drugbank:keggCompoundId ?compound .\n"
+ "}";
	
	  bw = new BufferedWriter(new FileWriter("fedx_fedsum_results.txt", true));
	  theDNFgrps =  BGPGroupGenerator.generateBgpGroups(S1);  // The time for DNF/bgp group generation is only around 3msec
	  TupleQuery tplQuery = repo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, ls4); 
  
   	long startTime = System.currentTimeMillis();
    TupleQueryResult res = tplQuery.evaluate();
    long count = 0;
   
    //System.out.println(endTime);
    
	
  /*  FedX fed = FederationManager.getInstance().getFederation();
	List<Endpoint> members = fed.getMembers();
	for (Endpoint e : members) 
	{
		MonitoringImpl.MonitoringInformation.class.
	}*/
   // MonitoringUtil.printMonitoringInformation();
  while (res.hasNext()) {
//	//System.out.println(": "+ res.next());
      res.next();
		count++;
	}
  long endTime = System.currentTimeMillis();
  long runTime = endTime-startTime;
  System.out.println(": Query exection time (msec):"+ (runTime));
 	bw.write("\t"+runTime);
 	bw.newLine();
	//  endTime = System.currentTimeMillis();
	//   System.out.println("Execution time(sec) : "+ (endTime-startTime)/1000);
	   System.out.println("Total Number of Records: " + count );
	   System.out.println("Done.");
	   bw.close();
	   System.exit(0);
	

	}
}
