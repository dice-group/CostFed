### CostFed: Cost-Based Query Optimization for SPARQL Endpoint Federation
 
 CostFed is an index-assisted federation engine for federated SPARQL query processing over multiple SPARQL endpoints. CostFed makes use of statistical information collected from endpoints to perform efficient source selection and cost-based query planning. In contrast to the state of the art, it relies on a non-linear model for the estimation of the selectivity of joins. Therewith, it is able to generate better plans than the state-of-the-art federation engines. In an experimental evaluation based on FedBench benchmark, we show
that CostFed is 3 to 121 times faster than the state of the art SPARQL endpoint federation engines. 

### How to Run CostFed?
* Checkout: the source code and import as new maven project. it will create three sub-projects, i.e, costfed, fex, and semagrow-bench. 
* Create Index: Since CostFed is an index-assisted appraoch, the first step is to generate an index for all the endpoints in hand. The index generation, updation is given costfed/src/main/java/org/aksw/simba/quetsal/util/TBSSSummariesGenerator.java. Note for FedBench, LargeRDFBench, the index is already given at costfed/summaries/sum-localhost.n3. 
* Configuration File: Set properties in /costfed/costfed.props or run with default
* Query Execution: costfed/src/main/java/org/aksw/simba/start/QueryEvaluation.java. Here you need to specify the URLs of the SPARQL endpoints which you want the given query to be federated and provide the configuration file, i.e., costfed.props as argument.  

### Used Benchmarks
The queries used in the evaluation can be downloaded from [FedBench](http://fedbench.fluidops.net/) and [LargeRDFBech](https://github.com/AKSW/largerdfbench) homepage. 

### Datasets Availability 

All the datasets and corresponding virtuoso SPARQL endpoints can be downloaded from the links given below. You may start a SPARQL endpoint from bin/start.bat (for windows) and bin/start_virtuoso.sh (for linux). 

| *Dataset*  | *Data-dump*  | *Windows Endpoint*  | *Linux Endpoint*  | *Local Endpoint Url*  | *Live Endpoint Url*|
|------------|--------------|---------------------|-------------------|-----------------------|--------------------|
| [ChEBI](https://www.ebi.ac.uk/chebi/) |[Download](https://drive.google.com/file/d/0B1tUDhWNTjO-Vk81dGVkNVNuY1E/edit?usp=sharing/ )| [Download](https://drive.google.com/file/d/0B1tUDhWNTjO-TUR6RF9jX2xoMFU/edit?usp=sharing/)|[Download](https://drive.google.com/file/d/0B1tUDhWNTjO-Wk5LeHBzMUd3VHc/edit?usp=sharing )|your.system.ip.address:8890/sparql | - |
| [DBPedia-Subset](http://DBpedia.org/) |[Download](https://drive.google.com/file/d/0B1tUDhWNTjO-QWk5MVJud3cxUXM/edit?usp=sharing/ )|  [Download](https://drive.google.com/file/d/0B1tUDhWNTjO-WjNkZEZrTTZzbW8/edit?usp=sharing/)|[Download](https://drive.google.com/file/d/0B1tUDhWNTjO-OEgyXzBUVmlMQlk/edit?usp=sharing )|your.system.ip.address:8891/sparql |http://dbpedia.org/sparql |
| [ DrugBank](http://www.drugbank.ca/)|[Download](https://drive.google.com/file/d/0B1tUDhWNTjO-cVp5QV9VUWRuYkk/edit?usp=sharing/ ) | [Download](https://drive.google.com/file/d/0B1tUDhWNTjO-QmMyOE9RWV9oNHM/edit?usp=sharing/ )| [Download](https://drive.google.com/file/d/0B1tUDhWNTjO-U0V5Y0xDWXhzam8/edit?usp=sharing/ )|your.system.ip.address:8892/sparql | http://wifo5-04.informatik.uni-mannheim.de/drugbank/sparql|
| [Geo Names](http://www.geonames.org/) |[Download](https://drive.google.com/file/d/0B1tUDhWNTjO-WEZZb2VwOG5vZkU/edit?usp=sharing/ ) | [Download](https://drive.google.com/file/d/0B1tUDhWNTjO-VC1HWmhBMlFncWc/edit?usp=sharing/ ) | [Download](https://drive.google.com/file/d/0B_MUFqryVpByd3hJcHBPeHZhejA/edit?usp=sharing/ ) |your.system.ip.address:8893/sparql | http://factforge.net/sparql|
| [Jamendo](http://dbtune.org/jamendo/ ) |[Download](https://drive.google.com/file/d/0B1tUDhWNTjO-cWpmMWxxQ3Z2eVk/edit?usp=sharing/ ) | [Download](https://drive.google.com/file/d/0B1tUDhWNTjO-YXV6U0ZzLUF0S0k/edit?usp=sharing/ ) | [Download](https://drive.google.com/file/d/0B1tUDhWNTjO-V3JMZjdfRkZxLUU/edit?usp=sharing/ ) |your.system.ip.address:8894/sparql  | http://dbtune.org/jamendo/sparql/|
| [KEGG](http://www.genome.jp/kegg/) |[Download](https://drive.google.com/file/d/0B1tUDhWNTjO-TUdUcllRMGVJaHM/edit?usp=sharing/ ) | [Download](https://drive.google.com/file/d/0B1tUDhWNTjO-c1BNQ0dVWTVkUEU/edit?usp=sharing/ )| [Download](https://drive.google.com/file/d/0B1tUDhWNTjO-R1dKbDlHNXZ6blk/edit?usp=sharing/ ) |your.system.ip.address:8895/sparql |http://cu.kegg.bio2rdf.org/sparql |
| [Linked MDB](http://linkedmdb.org/) |[Download](https://drive.google.com/file/d/0B1tUDhWNTjO-bU5VN25NLXZXU0U/edit?usp=sharing/ ) | [Download](https://drive.google.com/file/d/0B1tUDhWNTjO-eXpVSjd2Y25PaVk/edit?usp=sharing/ )| [Download](https://drive.google.com/file/d/0B1tUDhWNTjO-NjVTVERvajJUcGc/edit?usp=sharing/) |your.system.ip.address:8896/sparql |http://www.linkedmdb.org/sparql |
| [New York Times](http://data.nytimes.com/) |[ Download](https://drive.google.com/file/d/0B1tUDhWNTjO-dThoTm9DSmY4Wms/edit?usp=sharing/) | [Download](https://drive.google.com/file/d/0B1tUDhWNTjO-VDhmNWJmZVcybm8/edit?usp=sharing/ ) | [Download](https://drive.google.com/file/d/0B1tUDhWNTjO-RG9GeVdxbDR4YjQ/edit?usp=sharing/ ) |your.system.ip.address:8897/sparql | - |
| [Semantic Web Dog Food](http://data.semanticweb.org/) |[Download](https://drive.google.com/file/d/0B1tUDhWNTjO-RjBWZXYyX2FDT1E/edit?usp=sharing/ )| [Download](https://drive.google.com/file/d/0B1tUDhWNTjO-c2h4al9VREF6bDg/edit?usp=sharing/ ) | [Download](https://drive.google.com/file/d/0B1tUDhWNTjO-UW5HaF9rekdialU/edit?usp=sharing/ ) |your.system.ip.address:8898/sparql | http://data.semanticweb.org/sparql|
| [Affymetrix](http://download.bio2rdf.org/release/2/affymetrix/affymetrix.html)| [Download](https://drive.google.com/file/d/0B1tUDhWNTjO-eHVlZ1RyVVFJQU0/edit?usp=sharing/ )| [ Download](https://drive.google.com/file/d/0B1tUDhWNTjO-RnV4SWtKelJTb0U/edit?usp=sharing/)|[Download](https://drive.google.com/file/d/0B1tUDhWNTjO-Tm9oazNUdV9Cb1k/edit?usp=sharing )|your.system.ip.address:8899/sparql |http://cu.affymetrix.bio2rdf.org/sparql |

### Evaluation Results and Runtime Errors
 
We have compared 5 - [FedX](https://www.mpi-inf.mpg.de/~khose/publications/ISWC2011.pdf ), [SPLENDID](http://ceur-ws.org/Vol-782/GoerlitzAndStaab_COLD2011.pdf ), [ANAPSID](http://link.springer.com/chapter/10.1007%2F978-3-642-25073-6_2#page-1 ), [SemaGrow](http://dl.acm.org/citation.cfm?id=2814886),
[HiBISUCuS](http://svn.aksw.org/papers/2014/HiBISCuS_ESWC/public.pdf) - state-of-the-art SPARQL endpoint federation systems with CostFed. Our complete evaluation results can be downloaded from [here](https://github.com/AKSW/CostFed/blob/master/Results.xlsx). 
 

### Authors

  * Alexander Potocki (AKSW, University of Leipzig) 
  * [Muhammad Saleem](https://sites.google.com/site/saleemsweb/) (AKSW, University of Leipzig) 
  * [Tommaso Soru](http://tommaso-soru.it/) (AKSW, University of Leipzig)
  * [Olaf Hartig](http://olafhartig.de/)(Linköping University, Sweden)
  * [Axel-Cyrille Ngonga Ngomo](http://aksw.org/AxelNgonga.html) (AKSW, University of Leipzig)
 
We are especially thankful to Andreas Schwarte (fluid Operations, Germany), Olaf Görlitz (University Koblenz, Germany), and Angelos Charalambidis	(Institute of Informatics and Telecommunication, Paraskevi, Greece) for all their email conversations, feedbacks, and explanations. 	

