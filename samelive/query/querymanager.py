import json
import traceback
import requests
import socket
from math import ceil
import concurrent.futures

from samelive.utils.config import Config
from samelive.utils.helper import Helper

import tqdm
from rdflib import Graph, ConjunctiveGraph
from SPARQLWrapper import SPARQLWrapper, JSON, N3, XML


class Setup(object):
    def __init__(self):
        self.master_endpoint = Config.master_endpoint

    def populate(self, resources_list: list, endpoints_dict: dict = {}):
        """
        Populates a triplestore with resources and endpoints that will be used by the equivalent links search algorithm
        (:label: P1).
        :param resources_list: List of String, resources for which identity links are sought.
        :param endpoints_dict: Dict, a void:Dataset and its SPARQL endpoints.
        """
        try:
            sparql = SPARQLWrapper(self.master_endpoint)
            sparql.method = 'POST'
            sparql.setRequestMethod('postdirectly')
            sparql.setQuery("""
                PREFIX same: <https://ns.inria.fr/same/same.owl#>
                PREFIX owl: <http://www.w3.org/2002/07/owl#>
                PREFIX void: <http://rdfs.org/ns/void#>
                INSERT DATA {
                  GRAPH same:Q0 {
                    %s
                    same:Q0 same:hasIteration %d

                  }
                  GRAPH same:N {
                    %s
                  }
                }
            """ % ('.\n'.join(["<" + r + "> a same:Target" for r in resources_list]), 0,
                   ('.\n'.join(["<" + key + "> a void:Dataset ;\n void:sparqlEndpoint " + endpoints_dict[key]
                                for key in endpoints_dict]))))
            sparql.query()

            sparql.setQuery("""
                PREFIX same: <https://ns.inria.fr/same/same.owl#>
                WITH same:Q0
                INSERT {
                  ?IRITarget same:hasNamespace ?nstarget ;
                  same:hasAuthority ?auttarget ;
                  same:hasValueWithNoScheme ?noschemetarget
                } WHERE {
                  ?IRITarget a same:Target
                  BIND(REPLACE(STR(?IRITarget), "(#|/)[^#/]*$", "$1") as ?nstarget)
                  BIND(REPLACE(STR(?IRITarget), ".+://(.*)", "$1") as ?noschemetarget)
                  BIND(REPLACE(STR(?IRITarget), ".+://(.*?)/.*", "$1") as ?auttarget)
                }
            """)
            sparql.query()
        except Exception as err:
            traceback.print_tb(err)

    def populate_void_rkbexplorer(self):
        """
        Retrieves datasets information on the SPARQL endpoint of the voiD store and populates a triplestore with this
        data (:label: N1).
        """
        try:
            sparql = SPARQLWrapper("http://void.rkbexplorer.com/sparql")
            sparql.method = 'GET'
            sparql.setQuery("""
                PREFIX void: <http://rdfs.org/ns/void#>
                PREFIX dcterms: <http://purl.org/dc/terms/>
                CONSTRUCT {
                  ?URIDataset a void:Dataset .
                  ?URIDataset void:sparqlEndpoint ?endpoint
                } WHERE {
                  ?URIDataset a void:Dataset ;
                  dcterms:title ?title ;
                  void:sparqlEndpoint ?endpoint
                  FILTER(!isBlank(?URIDataset))
                }
            """)
            cg = ConjunctiveGraph()
            results = cg.parse(data=sparql.query().convert().decode('utf-8'), format="application/rdf+xml")
            prefixes = "PREFIX same: <https://ns.inria.fr/same/same.owl#>"
            # Helper.insert_graph(self.master_endpoint, results, 'same:N', prefixes)
            Helper.insert_graph(self.master_endpoint, results, '<https://ns.inria.fr/same/same.owl#voidStore>',
                                prefixes)

            # Remove dupplicated endpoints
            sparql = SPARQLWrapper(self.master_endpoint)
            sparql.method = 'POST'
            sparql.setRequestMethod('postdirectly')
            sparql.setQuery("""
                PREFIX void: <http://rdfs.org/ns/void#>
                PREFIX dcterms: <http://purl.org/dc/terms/>
                PREFIX same: <https://ns.inria.fr/same/same.owl#>
                WITH same:voidStore
                DELETE {
                  ?URIDataset2 a void:Dataset .
                  ?URIDataset2 void:sparqlEndpoint ?endpoint
                } WHERE {
                  ?URIDataset1 a void:Dataset ;
                  void:sparqlEndpoint ?endpoint .
                  ?URIDataset2 a void:Dataset ;
                  void:sparqlEndpoint ?endpoint
                  FILTER(!sameTerm(?URIDataset1, ?URIDataset2))
                }
            """)
            sparql.query()
        except Exception as err:
            traceback.print_tb(err)

    def populate_lodcloud(self):
        """
        Retrieves datasets information on the data of lod-cloud.net and populates a triplestore with this data
        (:label: N2).
        """
        try:
            headers = {'Accept': 'application/json'}
            # backup solution, the website lod-cloud.net is currently down
            response = requests.get("https://web.archive.org/web/20210814083458id_/https://lod-cloud.net/lod-data.json",
                                    headers=headers)
            lod_dump = json.loads(response.text)
            dataset_names = [name for name in lod_dump]
            data = []
            for d in dataset_names:
                if len(lod_dump[d]["sparql"]) == 0:
                    continue
                try:
                    tmp_dataset = [metadata["access_url"] for metadata in lod_dump[d]["other_download"]
                                   if metadata["media_type"] == "meta/void"][0]
                except Exception as err:
                    tmp_dataset = lod_dump[d]["website"]
                    if tmp_dataset is None or len(tmp_dataset) < 5:
                        tmp_dataset = lod_dump[d]["sparql"][0]["access_url"]+".dataset"

                tmp_dataset = "<" + tmp_dataset + ">"
                data.append(tmp_dataset + " a void:Dataset ;")
                data.append(" dcterms:title \"" + lod_dump[d]["title"].replace('"', '\\"') + "\" ;")
                data.append(" void:sparqlEndpoint <" + lod_dump[d]["sparql"][0]["access_url"] + "> .")

            prefixes = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " \
                       "\nPREFIX owl: <http://www.w3.org/2002/07/owl#> \n" \
                       "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> \nPREFIX void: <http://rdfs.org/ns/void#> \n" \
                       "PREFIX dcterms: <http://purl.org/dc/terms/> \n" \
                       "PREFIX ends: <http://labs.mondeca.com/vocab/endpointStatus#> " \
                       "\nPREFIX same: <https://ns.inria.fr/same/same.owl#>"
            # Helper.insert_array(self.master_endpoint, data, 'same:N', prefixes)
            Helper.insert_array(self.master_endpoint, data, '<https://ns.inria.fr/same/same.owl#LODCloud>', prefixes)

        except Exception as err:
            traceback.print_tb(err)

    def populate_datahub(self):
        """
        Retrieves datasets information on the data of old.datahub.io and populates a triplestore with this data
        (:label: N5).
        """
        # original query from: Buil-Aranda, C., Hogan, A., Umbrich, J., & Vandenbussche, P. Y. (2013, October).
        # SPARQL web-querying infrastructure: Ready for action?. In International Semantic Web Conference
        # (pp. 277-293). Springer, Berlin, Heidelberg.
        # https://old.datahub.io/api/3/search/resource?format=api/sparql&all_fields=1&limit=10000
        try:
            headers = {'Accept': 'application/json'}
            response = requests.get("https://old.datahub.io/api/3/search/resource?format="
                                    "sparql&all_fields=1&limit=10000", headers=headers)
            datahub_dump = json.loads(response.text)["results"]
            data = []
            for d in datahub_dump:
                data.append(" <" + d["url"].replace(' ', '') + ".dataset> a void:Dataset ;")
                data.append(" void:sparqlEndpoint <" + d["url"].replace(' ', '') + "> .")
            prefixes = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" \
                       "PREFIX owl: <http://www.w3.org/2002/07/owl#> \n" \
                       "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> \n" \
                       "PREFIX void: <http://rdfs.org/ns/void#> \nPREFIX dcterms: <http://purl.org/dc/terms/> \n" \
                       "PREFIX ends: <http://labs.mondeca.com/vocab/endpointStatus#>"
            Helper.insert_array(self.master_endpoint, data, '<https://ns.inria.fr/same/same.owl#DataHub>', prefixes)

        except Exception as err:
            traceback.print_tb(err)

    def populate_linkedwiki(self):
        """
        Retrieves datasets information on the data of linkedwiki and populates a triplestore with this data
        (:label: N4).
        """
        try:
            sparql = SPARQLWrapper(self.master_endpoint)
            sparql.method = 'POST'
            sparql.setRequestMethod('postdirectly')
            sparql.setQuery("""
                PREFIX void: <http://rdfs.org/ns/void#>
                PREFIX dcterms: <http://purl.org/dc/terms/>
                PREFIX dcat: <http://www.w3.org/ns/dcat#>
                PREFIX same: <https://ns.inria.fr/same/same.owl#>
                INSERT {
                  GRAPH same:LinkedWiki {
                    ?URIdataset a void:Dataset .
                    ?URIdataset dcterms:title ?title .
                    ?URIdataset void:sparqlEndpoint ?endpoint
                  }
                } WHERE {
                  SERVICE <https://linkedwiki.com/sparql> {
                    ?URIdataset dcat:distribution ?distrib .
                    OPTIONAL { ?URIdataset <http://purl.org/dc/terms/title> ?title }
                    ?distrib dcat:accessURL ?endpoint 
                  }
                }
            """)
            sparql.query()

        except Exception as err:
            traceback.print_tb(err)

    def populate_umakata(self):
        """
        Retrieves datasets information on the API of yummydata and populates a triplestore with this data
        (:label: N3).
        """
        try:
            headers = {'Accept': 'application/json'}
            response = requests.get("https://yummydata.org/api/endpoint/search", headers=headers)
            umakata_dump = json.loads(response.text)

            # TODO transition to Python 3.9
            datasets_void = ["<" + Helper().removesuffix(Helper().removesuffix(e["endpoint_url"], "virtuoso/sparql"),
                                                         "sparql") + ".well-known/void>"
                             for e in umakata_dump if e["evaluation"]["void"] is True]
            datasets_no_void = ["<" + e["description_url"] + ">" for e in umakata_dump if e["evaluation"]["void"]
                                is False]
            data = []
            for e in umakata_dump:
                if e["evaluation"]["void"] is True:
                    tmp_dataset = Helper().removesuffix(Helper().removesuffix(e["endpoint_url"], "virtuoso/sparql"),
                                                        "sparql") + ".well-known/void"
                if e["evaluation"]["void"] is False:
                    tmp_dataset = e["description_url"]
                tmp_dataset_status = "<" + tmp_dataset + ".status>"
                tmp_dataset = "<" + tmp_dataset + ">"
                data.append(tmp_dataset_status + " a ends:EndpointStatus .")
                data.append(tmp_dataset + " a void:Dataset ;")
                data.append(" dcterms:title \"" + e["name"].replace('"', '\\"') + "\" ;")
                data.append(" void:sparqlEndpoint <" + e["endpoint_url"] + "> ;")
                data.append(" ends:status " + tmp_dataset_status + " .")
                data.append(tmp_dataset_status + " ends:statusIsAvailable {} .".format(e["evaluation"]["alive"]))
                # ends namespace https://labs.mondeca.com/vocab/endpointStatus/
            prefixes = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" \
                       "PREFIX owl: <http://www.w3.org/2002/07/owl#> \n" \
                       "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> \n" \
                       "PREFIX void: <http://rdfs.org/ns/void#> \nPREFIX dcterms: <http://purl.org/dc/terms/> \n" \
                       "PREFIX ends: <http://labs.mondeca.com/vocab/endpointStatus#>"
            # Helper.insert_array(self.master_endpoint, data, 'same:N', prefixes)
            Helper.insert_array(self.master_endpoint, data, '<https://ns.inria.fr/same/same.owl#Yummydata>', prefixes)

        except Exception as err:
            traceback.print_tb(err)

    def cleanup_datasets(self):
        """
        (:label: CN)
        """
        try:
            sparql = SPARQLWrapper(self.master_endpoint)
            sparql.method = 'POST'
            sparql.setRequestMethod('postdirectly')
            # Iteration  difficult to convert in integer in Python
            sparql.setQuery("""
                PREFIX same: <https://ns.inria.fr/same/same.owl#>
                PREFIX void: <http://rdfs.org/ns/void#>
                INSERT  {
                  GRAPH same:N {
                    ?d1 a void:Dataset ;
                    void:sparqlEndpoint ?e1 ;
                    ?p1 ?o1
                  }
                } WHERE {
                  GRAPH same:Yummydata {
                    ?d1 a void:Dataset ;
                    void:sparqlEndpoint ?e1 ;
                    ?p1 ?o1
                  }
                  FILTER(!EXISTS {
                    GRAPH same:N {
                      ?d2 void:sparqlEndpoint ?e2 ;
                    } 
                    FILTER(?e1 = ?e1) })
                }
            """)
            sparql.query()

            sparql.setQuery("""
                PREFIX same: <https://ns.inria.fr/same/same.owl#>
                PREFIX void: <http://rdfs.org/ns/void#>
                INSERT {
                  GRAPH same:N {
                    ?d1 a void:Dataset ;
                    void:sparqlEndpoint ?e1 ;
                    ?p1 ?o1
                  }
                } WHERE {
                  GRAPH same:LinkedWiki {
                    ?d1 a void:Dataset ;
                    void:sparqlEndpoint ?e1 ;
                    ?p1 ?o1
                  }
                  FILTER(!EXISTS {
                    GRAPH same:N {
                      ?d2 void:sparqlEndpoint ?e1 ;
                    }})
                }
            """)
            sparql.query()

            sparql.setQuery("""
                PREFIX same: <https://ns.inria.fr/same/same.owl#>
                PREFIX void: <http://rdfs.org/ns/void#>
                INSERT {
                  GRAPH same:N {
                    ?d1 a void:Dataset ;
                    void:sparqlEndpoint ?e1 ;
                    ?p1 ?o1
                  }
                } WHERE {
                  GRAPH same:LODCloud {
                    ?d1 a void:Dataset ;
                    void:sparqlEndpoint ?e1 ;
                    ?p1 ?o1
                  }
                  FILTER(!EXISTS {
                    GRAPH same:N {
                      ?d2 void:sparqlEndpoint ?e1 ;
                    }})
                }
            """)
            sparql.query()

            sparql.setQuery("""
                PREFIX same: <https://ns.inria.fr/same/same.owl#>
                PREFIX void: <http://rdfs.org/ns/void#>
                INSERT {
                  GRAPH same:N {
                    ?d1 a void:Dataset ;
                    void:sparqlEndpoint ?e1 ;
                    ?p1 ?o1
                  }
                } WHERE {
                  GRAPH same:DataHub {
                    ?d1 a void:Dataset ;
                    void:sparqlEndpoint ?e1 ;
                    ?p1 ?o1
                  }
                  FILTER(!EXISTS {
                    GRAPH same:N {
                      ?d2 void:sparqlEndpoint ?e1 ;
                    }})
                }
            """)
            sparql.query()

            sparql.setQuery("""
                PREFIX same: <https://ns.inria.fr/same/same.owl#>
                PREFIX void: <http://rdfs.org/ns/void#>
                INSERT {
                  GRAPH same:N {
                    ?d1 a void:Dataset ;
                    void:sparqlEndpoint ?e1 ;
                    ?p1 ?o1
                  }
                } WHERE {
                  GRAPH same:voidStore {
                    ?d1 a void:Dataset ;
                    void:sparqlEndpoint ?e1 ;
                    ?p1 ?o1
                  }
                  FILTER(!EXISTS {
                    GRAPH same:N {
                      ?d2 void:sparqlEndpoint ?e1 ;
                    }})
                }
            """)
            sparql.query()

            sparql.setQuery("""
                PREFIX same: <https://ns.inria.fr/same/same.owl#>
                PREFIX void: <http://rdfs.org/ns/void#>
                WITH same:N
                DELETE {
                  ?d2 ?p2 ?o2
                } WHERE {
                  ?d1 a void:Dataset ;
                  void:sparqlEndpoint ?e .
                  ?d2 a void:Dataset ;
                  void:sparqlEndpoint ?e ;
                  ?p2 ?o2 .
                  FilTER(?d1 != ?d2)
                  FILTER(STRLEN(STR(?d1)) > STRLEN(STR(?d2)))
                }
            """)
            sparql.query()

        except Exception as err:
            traceback.print_tb(err)

    def setup_vocabulary(self):
        """
        Initalizes the vocabulary used by the identity link search algorithm in a triplestore.
        """
        try:
            sparql = SPARQLWrapper(self.master_endpoint)
            sparql.method = 'POST'
            sparql.setRequestMethod('postdirectly')
            # Iteration  difficult to convert in integer in Python
            sparql.setQuery("""
                PREFIX same: <https://ns.inria.fr/same/same.owl#>
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX rdfg: <http://www.w3.org/2004/03/trix/rdfg-1>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                PREFIX owl: <http://www.w3.org/2002/07/owl#>
                PREFIX void: <http://rdfs.org/ns/void#>
                PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                PREFIX ends: <http://labs.mondeca.com/vocab/endpointStatus#>
                PREFIX prov: <http://www.w3.org/ns/prov#>
                PREFIX fno: <https://w3id.org/function/ontology#>
                PREFIX dcterms: <http://purl.org/dc/terms/>
                INSERT DATA {
                  GRAPH same:default {
                    same:Target a owl:Class, foaf:Document ;
                    rdfs:label "Targeted resource for discovering equivalence relationships." .
                    same:Rotten a owl:Class ;
                    a foaf:Document ;
                    rdfs:label "Resource that is source of potentially erroneous equivalence relationships." .
                    same:hasIteration a owl:DatatypeProperty ;
                    rdfs:domain rdfg:Graph ;
                    rdfs:range xsd:decimal ;
                    rdfs:label "Iteration of the equivalence relationships algorithm during which the named graph was generated." .
                    same:hasNamespace a owl:DatatypeProperty ;
                    rdfs:domain rdfs:Class ;
                    rdfs:range xsd:string ;
                    rdfs:label "Namespace of the resource." .
                    same:hasAuthority a owl:DatatypeProperty ;
                    rdfs:domain rdfs:Class ;
                    rdfs:range xsd:string ;
                    rdfs:label "Authority of the resource." .
                    same:hasValueWithNoScheme a owl:DatatypeProperty ;
                    rdfs:domain rdfs:Class ;
                    rdfs:range xsd:string ;
                    rdfs:label "String value of an URL without the scheme component." .
                    same:hasSchemaFor a rdf:ObjectProperty ;
                    rdfs:domain void:Dataset ;
                    rdfs:range rdfs:Class ;
                    rdfs:label "Specifies that the void:Dataset includes the resource's schemas." .
                    same:statementInDataset a rdf:ObjectProperty ;
                    rdfs:domain rdf:Statement ;
                    rdfs:range void:Dataset ;
                    rdfs:label "Points to the void:Dataset that a statement is a part of." .
                    same:votingType a owl:ObjectProperty ;
                    rdfs:domain rdfs:Class ;
                    rdfs:range rdfs:Class ;
                    rdfs:label "rdf:type of the resource determined after voting." .
                    same:N a rdfg:Graph ;
                    rdfs:label "Named graph that contains information about the void:Dataset resources." .
                    same:Properties a rdfg:Graph ;
                    rdfs:label "Named graph that contains asserted owl:InverseFunctionalProperty and owl:FunctionalProperty." .
                    same:PropertiesNotDeferenced a rdfg:Graph ;
                    rdfs:label "Named graph that contains not deferenced properties after the LOAD clause." .
                    same:Q-1 a rdfg:Graph ;
                    rdfs:label "Named graph that contains the same:Rotten resources." .
                    same:Q0 a rdfg:Graph ;
                    rdfs:label "Named graph that contains the starting same:Target." .
                    same:triplesInGraph a owl:DatatypeProperty ;
                    rdfs:domain rdfg:Graph ;
                    rdfs:range xsd:integer ;
                    rdfs:label "Number of triples in the named graph." .
                    same:sameAsInGraph a owl:DatatypeProperty ;
                    rdfs:domain rdfg:Graph ;
                    rdfs:range xsd:integer ;
                    rdfs:label "Number of owl:sameAs triples in the named graph." .
                    same:targetsInGraph a owl:DatatypeProperty ;
                    rdfs:domain rdfg:Graph ;
                    rdfs:range xsd:integer ;
                    rdfs:label "Number of resources of type same:Target in the named graph." .
                    same:totalTargets a owl:DatatypeProperty ;
                    rdfs:range xsd:integer ;
                    rdfs:label "Number of resources of type same:Target." .
                    same:totalRottens a owl:DatatypeProperty ;
                    rdfs:range xsd:integer ;
                    rdfs:label "Number of resources of type same:Rotten." .
                    same:availableDatasets a owl:DatatypeProperty ;
                    rdfs:range xsd:integer ;
                    rdfs:label "Number of datasets answering to HTTP requests." .
                    same:totalLoadedRDFDocuments a owl:DatatypeProperty ;
                    rdfs:range xsd:integer ;
                    rdfs:label "Number of RDF documents loaded with the clause UPDATE LOAD." .
                    same:totalNotLoadedRDFDocuments a owl:DatatypeProperty ;
                    rdfs:range xsd:integer ;
                    rdfs:label "Number of RDF documents not loaded with the clause UPDATE LOAD." .
                    # In our case resources are owl:InverseFunctionalProperty and owl:FunctionalProperty
                    same:totalRDFDocuments a owl:DatatypeProperty ;
                    rdfs:range xsd:integer ;
                    rdfs:label "Number of RDF documents on which are defined resources." .
                    same:totalDeferencedInverseFunctionalProperties a owl:DatatypeProperty ;
                    rdfs:range xsd:integer ;
                    rdfs:label "Number of owl:InverseFunctionalProperty properties for which it was not possible to identify schemas by loading their reference RDF document." .
                    same:totalNotDeferencedFunctionalProperties a owl:DatatypeProperty ;
                    rdfs:range xsd:integer ;
                    rdfs:label "Number of owl:FunctionalProperty properties for which it was not possible to identify schemas by loading their reference RDF document." .
                    same:totalDeferencedFunctionalProperties a owl:DatatypeProperty ;
                    rdfs:range xsd:integer ;
                    rdfs:label "Number of owl:FunctionalProperty properties for which it was possible to identify schemas by loading their reference RDF document." .
                    same:inNbOfDataset a owl:DatatypeProperty ;
                    rdfs:domain rdfs:Class ;
                    rdfs:range xsd:integer ;
                    rdfs:label "Number of void:Dataset that a rdfs:Class is part of." .
                    same:nbOfTimesDefinedAsInverseFunctionalProperty a owl:DatatypeProperty ;
                    rdfs:domain rdf:Property ;
                    rdfs:range xsd:integer ;
                    rdfs:label "Number of time a property is defined as owl:InverseFunctionalProperty on void:Dataset that include the property's schema." .
                    same:nbOfTimesDefinedAsFunctionalProperty a owl:DatatypeProperty ;
                    rdfs:domain rdf:Property ;
                    rdfs:range xsd:integer ;
                    rdfs:label "Number of time a property is defined as owl:FunctionalProperty on void:Dataset that include the property's schema." .
                    same:S1 a fno:Function, prov:Entity ;
                    dcterms:description "Retrieves owl:sameAs relationships." .
                    same:IFP2 a fno:Function, prov:Entity ;
                    dcterms:description "Computes owl:sameAs relationships with (inverse) functional properties patterns." .
                    same:valuesIsAvailable a owl:DatatypeProperty ;
                    rdfs:domain ends:EndpointStatus ;
                    rdfs:range 	xsd:boolean ;
                    rdfs:label "Describes whether a void:Dataset endpoint support the VALUES clause or not." .
                    same:supportsNonASCIICharacters a owl:DatatypeProperty ;
                    rdfs:domain ends:EndpointStatus ;
                    rdfs:range 	xsd:boolean ;
                    rdfs:label "Describes whether a void:Dataset endpoint support non-ASCII characters or not." .
                    same:hasResultsLimit a owl:DatatypeProperty ;
                    rdfs:domain ends:EndpointStatus ;
                    rdfs:range xsd:integer ;
                    rdfs:label "Results limit returned by an endpoint." .
                  }
                }
            """)
            sparql.query()

        except Exception as err:
            traceback.print_tb(err)

    def load_vocabularies_functionalproperties(self):
        """
        Retrieves RDF documents of alleged (inverse) functional properties by using their namespaces
        (:label: LDD-(I)FP1).
        """
        sparql = SPARQLWrapper(self.master_endpoint)
        sparql.method = 'POST'
        sparql.setReturnFormat(JSON)
        sparql.setQuery("""
            PREFIX same: <https://ns.inria.fr/same/same.owl#>
            SELECT DISTINCT ?nsp
            FROM same:Properties
            WHERE { 
              ?p a ?property ;
              same:hasNamespace ?nsp
              FILTER(?property IN (owl:InverseFunctionalProperty, owl:FunctionalProperty))
            }
        """)
        try:
            json = sparql.query().convert()["results"]["bindings"]
            query_pattern = ["LOAD SILENT <" + j["nsp"]["value"] + "> INTO GRAPH kg:default" for j in json]
            sparql.method = 'POST'
            sparql.setRequestMethod('postdirectly')
            sparql.setQuery("""
                %s
            """ % ';\n'.join(query_pattern))
            sparql.query()

            sparql.setQuery("""
                PREFIX same: <https://ns.inria.fr/same/same.owl#>
                PREFIX kg: <http://ns.inria.fr/corese/kgram/>	

                DELETE { GRAPH ?g { ?x ?p ?y } }
                INSERT { GRAPH kg:default { ?x ?p ?y } }
                WHERE { GRAPH ?g { ?x ?p ?y } 
                        FILTER(!STRSTARTS(STR(?g), STR(same:)) &&
                               !STRSTARTS(STR(?g), STR(kg:)))
                }
            """)
            sparql.query()
        except Exception as err:
            traceback.print_tb(err)


class LocalManipulation(object):
    def __init__(self):
        self.master_endpoint = Config.master_endpoint

    def get_targets(self, iterator: int = 0) -> [str]:
        """
        Retrieves the resources from a local endpoint the same:Target of the iteration it.
        :param iterator: int, iteration of the algorithm (:label: T1).
        :return: List of String, all the same:Target of the iteration it.
        """
        sparql = SPARQLWrapper(self.master_endpoint)
        sparql.method = 'POST'
        sparql.setReturnFormat(JSON)
        sparql.setQuery("""
            PREFIX same: <https://ns.inria.fr/same/same.owl#>
            SELECT ?IRITarget
            FROM same:Q""" + str(iterator - 1) + """
            WHERE {
              ?IRITarget a same:Target
            }
        """)
        resources = []
        try:
            json = sparql.query().convert()["results"]["bindings"]
            resources = [j["IRITarget"]["value"] for j in json]
        except Exception as err:
            traceback.print_tb(err)
        return resources

    # TODO generalize
    def get_datasets(self) -> dict:
        """
        Returns all the available datasets and the endpoint where we can reach them.
        :return: Dict, key is the dataset and the value is the endpoint.
        """
        sparql = SPARQLWrapper(self.master_endpoint)
        sparql.method = 'POST'
        sparql.setReturnFormat(JSON)
        sparql.setQuery("""
            PREFIX void: <http://rdfs.org/ns/void#>
            PREFIX ends: <http://labs.mondeca.com/vocab/endpointStatus#>
            PREFIX same: <https://ns.inria.fr/same/same.owl#>
            SELECT ?dataset ?endpoint
            FROM same:N
            WHERE {
              ?dataset void:sparqlEndpoint ?endpoint ;
              ends:status ?status .
              ?status ends:statusIsAvailable true
            }
        """)
        dic_datasets = {}
        try:
            json = sparql.query().convert()["results"]["bindings"]
            for j in json:
                dic_datasets.update({j["dataset"]["value"]: j["endpoint"]["value"]})
        except Exception as err:
            traceback.print_tb(err)
        return dic_datasets

    def get_datasets_with_limits(self) -> dict:
        """
        Returns all the available datasets and the endpoint where we can reach them.
        :return: Dict, key is the dataset and the value is a list containing the endpoint and the limit of results.
        """
        sparql = SPARQLWrapper(self.master_endpoint)
        sparql.method = 'POST'
        sparql.setReturnFormat(JSON)
        sparql.setQuery("""
            PREFIX void: <http://rdfs.org/ns/void#>
            PREFIX ends: <http://labs.mondeca.com/vocab/endpointStatus#>
            PREFIX same: <https://ns.inria.fr/same/same.owl#>
            SELECT ?dataset ?endpoint ?limit
            FROM same:N
            WHERE {
              ?dataset void:sparqlEndpoint ?endpoint ;
              ends:status ?status .
              ?status ends:statusIsAvailable true ;
              ?status same:hasResultsLimit ?limit
            }
        """)
        dic_datasets = {}
        try:
            json = sparql.query().convert()["results"]["bindings"]
            for j in json:
                dic_datasets.update({j["dataset"]["value"]: [j["endpoint"]["value"], j["limit"]["value"]]})
        except Exception as err:
            traceback.print_tb(err)
        return dic_datasets

    def compute_inversefunctionalproperty(self, iterator: int = 1):
        try:
            sparql = SPARQLWrapper(self.master_endpoint)
            sparql.method = 'POST'
            sparql.setRequestMethod('postdirectly')
            sparql.setQuery("""
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                INSERT {
                  ?URITarget1 owl:sameAs ?URITarget2 .
                  ?URITarget2 owl:sameAs ?URITarget1
                } WHERE {
                  ?p a owl:InverseFunctionalProperty .
                  ?URITarget1 ?p ?v .
                  ?URITarget2 ?p ?v .
                  FILTER (?URITarget1 != ?URITarget2)
                }
            """)
            sparql.query()

        except Exception as err:
            traceback.print_tb(err)

    def voting_functionalproperties(self):
        """
        Performs voting on the type of (inverse) functional properties (:label: V-(I)FP1).
        """
        try:
            sparql = SPARQLWrapper(self.master_endpoint)
            sparql.method = 'POST'
            sparql.setRequestMethod('postdirectly')
            sparql.setQuery("""
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX owl: <http://www.w3.org/2002/07/owl#>
                PREFIX same: <https://ns.inria.fr/same/same.owl#>
                
                INSERT {
                  GRAPH same:PropertiesNotDeferencedStatistics {
                    ?propertyNotDefined same:inNbOfDataset ?nbTotal 
                  }
                } WHERE {
                  select (count(?dataset) as ?nbTotal) ?propertyNotDefined where {
                    Graph same:PropertiesNotDeferenced {
                      ?propertyNotDefined a ?type1
                    }
                    Graph same:Properties {
                      <<?propertyNotDefined a ?type>> same:statementInDataset ?dataset
                    }
                  } group by ?propertyNotDefined
                }
            """)
            sparql.query()

            sparql.setQuery("""
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX owl: <http://www.w3.org/2002/07/owl#>
                PREFIX same: <https://ns.inria.fr/same/same.owl#>

                INSERT {
                  GRAPH same:PropertiesNotDeferencedStatistics {
                    ?propertyDefinedAsFunctionalProperty same:nbOfTimesDefinedAsFunctionalProperty ?nbDefinedAsFunctionalProperty
                  }
                } WHERE {
                  select (count(?dataset) as ?nbDefinedAsFunctionalProperty) ?propertyDefinedAsFunctionalProperty where {
                    Graph same:PropertiesNotDeferenced {
                      ?propertyDefinedAsFunctionalProperty a ?type1
                    }

                    Graph same:Properties {
                      <<?propertyDefinedAsFunctionalProperty a owl:FunctionalProperty>> same:statementInDataset ?dataset .
                      ?dataset same:hasSchemaFor ?propertyDefinedAsFunctionalProperty
                    }
                  } group by ?propertyDefinedAsFunctionalProperty
                }
            """)
            sparql.query()

            sparql.setQuery("""
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX owl: <http://www.w3.org/2002/07/owl#>
                PREFIX same: <https://ns.inria.fr/same/same.owl#>

                INSERT {
                  GRAPH same:PropertiesNotDeferencedStatistics {
                    ?propertyDefinedAsInverseFunctionalProperty same:nbOfTimesDefinedAsInverseFunctionalProperty ?nbDefinedAsInverseFunctionalProperty
                  }
                } WHERE {
                  select (count(?dataset) as ?nbDefinedAsInverseFunctionalProperty) ?propertyDefinedAsInverseFunctionalProperty where {
                    Graph same:PropertiesNotDeferenced {
                      ?propertyDefinedAsInverseFunctionalProperty a ?type1
                    }
                    Graph same:Properties {
                      <<?propertyDefinedAsInverseFunctionalProperty a owl:InverseFunctionalProperty>> same:statementInDataset ?dataset .
                      ?dataset same:hasSchemaFor ?propertyDefinedAsInverseFunctionalProperty
                    }
                  } group by ?propertyDefinedAsInverseFunctionalProperty
                }
            """)
            sparql.query()

            sparql.setQuery("""
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX owl: <http://www.w3.org/2002/07/owl#>
                PREFIX same: <https://ns.inria.fr/same/same.owl#>

                INSERT {
                  GRAPH same:PropertiesNotDeferencedStatistics {
                    ?propertyNotDefinedWithSchema same:inNbOfDatasetWithSchema ?nbTotalSchemaFor
                  }
                } WHERE {
                  select (count(?dataset) as ?nbTotalSchemaFor) ?propertyNotDefinedWithSchema where {
                    Graph same:PropertiesNotDeferenced {
                      ?propertyNotDefinedWithSchema a ?type
                    }
                    Graph same:Properties {
                      ?dataset same:hasSchemaFor ?propertyNotDefinedWithSchema
                    }
                  } group by ?propertyNotDefinedWithSchema
                }
            """)
            sparql.query()

            sparql.setQuery("""
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    PREFIX owl: <http://www.w3.org/2002/07/owl#>
                    PREFIX same: <https://ns.inria.fr/same/same.owl#>
                    PREFIX kg: <http://ns.inria.fr/corese/kgram/>

                    INSERT {
                      Graph kg:default {
                        ?property same:votingType ?votingType
                      }
                    } WHERE {
                      GRAPH same:PropertiesNotDeferencedStatistics {
                        ?property same:nbOfTimesDefinedAsInverseFunctionalProperty ?nbDefinedAsInverseFunctionalProperty .
                        ?property same:inNbOfDatasetWithSchema ?total
                      }
                      BIND(IF(?nbDefinedAsInverseFunctionalProperty >= ?total/2, owl:InverseFunctionalProperty, false)  as ?votingType)
                      FILTER(?votingType != false)
                    }
                    """)
            sparql.query()

            sparql.setQuery("""
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    PREFIX owl: <http://www.w3.org/2002/07/owl#>
                    PREFIX same: <https://ns.inria.fr/same/same.owl#>
                    PREFIX kg: <http://ns.inria.fr/corese/kgram/>

                    INSERT {
                      Graph kg:default {
                        ?property same:votingType ?votingType
                      }
                    } WHERE {
                      GRAPH same:PropertiesNotDeferencedStatistics {
                        ?property same:nbOfTimesDefinedAsFunctionalProperty ?nbDefinedAsFunctionalProperty .
                        ?property same:inNbOfDatasetWithSchema ?total
                      }
                      BIND(IF(?nbDefinedAsFunctionalProperty >= ?total/2, owl:FunctionalProperty, false)  as ?votingType)
                      FILTER(?votingType != false)
                    }
                    """)
            sparql.query()

        except Exception as err:
            traceback.print_tb(err)


class EndpointExploration(object):

    def __init__(self):
        self.master_endpoint = Config.master_endpoint
        self.timeout = Config.timeout
        self.IS_CORESE_ENGINE = Config.IS_CORESE_ENGINE
        self.NON_ASCII_CHARACTERS_HANDLING = Config.NON_ASCII_CHARACTERS_HANDLING

    def optimize_remote_queries(self, function, iterator: int = 1):
        """
        Allows to handle bindings with the VALUES clause and non-ASCII characters when generating SPARQL queries for
        remote endpoints. Then this function will execute these SPARQL queries.
        :param function: Function used to generate patterns of SPARQL queries (the function may return a String or tuple
        of Strings).
        :param iterator: int, iteration of the algorithm.
        """
        try:
            sparql = SPARQLWrapper(self.master_endpoint)
            sparql.method = 'POST'
            sparql.setRequestMethod('postdirectly')
            # Optimizations with the Corese engine
            if self.IS_CORESE_ENGINE:
                for query in Helper.non_ascii_characters_handling(function, iterator=iterator,
                                                                  handle_non_ascii=self.NON_ASCII_CHARACTERS_HANDLING,
                                                                  # Replace it with @bind in older versions of
                                                                  # Corese
                                                                  sparql_annotations="@binding kg:values",
                                                                  dataset_options="same:valuesIsAvailable true"):
                    sparql.setQuery(query)
                    sparql.query()

                # Bindings with FILTER
                for query in Helper.non_ascii_characters_handling(function, iterator=iterator,
                                                                  handle_non_ascii=self.NON_ASCII_CHARACTERS_HANDLING,
                                                                  # Replace it with @bind in older versions of
                                                                  # Corese
                                                                  sparql_annotations="@binding kg:filter",
                                                                  dataset_options="same:valuesIsAvailable false"):
                    sparql.setQuery(query)
                    sparql.query()

            # Default behavior for other triplestores
            else:
                for query in Helper.non_ascii_characters_handling(function, iterator=iterator,
                                                                  handle_non_ascii=self.NON_ASCII_CHARACTERS_HANDLING):
                    sparql.setQuery(query)
                    sparql.query()

        except Exception as err:
            traceback.print_tb(err)

    def _generate_query_pattern_sameas(self, iterator: int, sparql_annotations: str = "", dataset_options: str = "",
                                       target_options="FILTER(!REGEX(str(?IRITarget), \"[^\\\\x00-\\\\x7F]\", \"i\"))"):
        """
        Generate the query used to retrieve owl:sameAs relationships (:label: S1).
        :param iterator: int, iteration of the algorithm.
        :param sparql_annotations: String, SPARQL Annotations of the Corese engine
        (https://ns.inria.fr/sparql-extension/event.html#event).
        :param dataset_options: String, Options on the available SPARQL endpoints.
        :param target_options: String, Options on the same:Target resources.
        :return: String, SPARQL query used to retrieve owl:sameAs relationships.
        """
        query = """
                    PREFIX owl: <http://www.w3.org/2002/07/owl#>
                    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                    PREFIX rdfg: <http://www.w3.org/2004/03/trix/rdfg-1>
                    PREFIX void: <http://rdfs.org/ns/void#>
                    PREFIX ends: <http://labs.mondeca.com/vocab/endpointStatus#>
                    PREFIX prov: <http://www.w3.org/ns/prov#>
                    PREFIX fno: <https://w3id.org/function/ontology#>
                    PREFIX same: <https://ns.inria.fr/same/same.owl#>
                    PREFIX dcterms: <http://purl.org/dc/terms/>
                    %s
                    INSERT {
                      ?ngraph a rdfg:Graph, prov:Entity .

                      GRAPH ?ngraph {
                        ?IRITarget owl:sameAs ?IRIy .
                        ?IRIy owl:sameAs ?IRITarget
                      }

                      ?execution a fno:Execution, prov:Activity ;
                      fno:executes same:S1 ;
                      dcterms:date ?date .

                      ?ngraph prov:wasGeneratedBy ?execution .

                      GRAPH same:Q%s {
                        ?IRIy a same:Target ;
                        void:inDataset ?dataset ;
                        same:hasNamespace ?nsy ;
                        same:hasAuthority ?auty ;
                        same:hasValueWithNoScheme ?noschemey
                      }
                      same:Q%s same:hasIteration %d .
                      ?ngraph same:hasIteration %d
                    } WHERE {
                      GRAPH same:Q%s  {
                        ?IRITarget a same:Target
                        %s
                      }
                      GRAPH same:N {
                        ?dataset void:sparqlEndpoint ?endpoint ;
                        ends:status ?status .
                        ?status ends:statusIsAvailable true ;
                        %s
                      }
                      # Small issue by using prefixes in Corese with clause SERVICE
                      SERVICE ?endpoint {
                        { ?IRITarget <http://www.w3.org/2002/07/owl#sameAs> ?y } 
                        UNION 
                        { ?y <http://www.w3.org/2002/07/owl#sameAs> ?IRITarget }
                        FILTER(!isBlank(?y))
                      }
                      BIND(IRI(concat(str(?endpoint), '#', %s, 'S1')) as ?ngraph)
                      BIND(IRI(concat(str(?endpoint), '#', %s, 'S1', 'Execution')) as ?execution)
                      # Data clearing some IRI are represented as a String
                      BIND(IRI(?y) as ?IRIy)
                      BIND(REPLACE(STR(?y), "(#|/)[^#/]*$", "$1") as ?nsy)
                      BIND(REPLACE(STR(?y), ".+://(.*?)/.*", "$1") as ?auty)
                      BIND(REPLACE(STR(?y), ".+://(.*)", "$1") as ?noschemey)
                      # No rotten links
                      FILTER(!EXISTS { GRAPH <Q-1> { ?y a same:Rotten }})
                      FILTER(!EXISTS { ?y a same:Target })
                      BIND(xsd:dateTime(NOW()) AS ?date)
                    }
                """ % (sparql_annotations, str(iterator), str(iterator), iterator, iterator, str(iterator - 1),
                       target_options, dataset_options, str(iterator), str(iterator))
        return query

    def retrieve_functionalproperties_schemas(self):
        """
        Retrieves alleged (inverse) functional properties (:label: G-(I)FP1).
        """
        try:
            sparql = SPARQLWrapper(self.master_endpoint)
            sparql.method = 'POST'
            sparql.setRequestMethod('postdirectly')
            sparql.setQuery("""
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX owl: <http://www.w3.org/2002/07/owl#>
                PREFIX void: <http://rdfs.org/ns/void#>
                PREFIX ends: <http://labs.mondeca.com/vocab/endpointStatus#>
                PREFIX same: <https://ns.inria.fr/same/same.owl#>
                INSERT {
                  GRAPH same:Properties {
                    ?URIInvFunctProperty a owl:InverseFunctionalProperty ;
                    same:hasNamespace ?nsifp .
                    << ?URIInvFunctProperty a owl:InverseFunctionalProperty >> same:statementInDataset ?dataset .
                    ?URIFunctProperty a owl:FunctionalProperty ;
                    same:hasNamespace ?nsfp .
                    << ?URIFunctProperty a owl:FunctionalProperty >> same:statementInDataset ?dataset
                  }
                } WHERE {
                  GRAPH same:N {
                    ?dataset void:sparqlEndpoint ?endpoint ;
                    ends:status ?status .
                    ?status ends:statusIsAvailable true
                  }
                  
                  SERVICE ?endpoint {
                    {
                      ?InvFunctProperty a owl:InverseFunctionalProperty
                      FILTER(!isBlank(?InvFunctProperty))
                    } UNION {
                      ?FunctProperty a owl:FunctionalProperty
                      FILTER(!isBlank(?FunctProperty))
                    }
                  }
                  BIND(REPLACE(STR(?InvFunctProperty), "(#|/)[^#/]*$", "$1") as ?nsifp)
                  BIND(REPLACE(STR(?FunctProperty), "(#|/)[^#/]*$", "$1") as ?nsfp)
                  # Correct IRI stored as String
                  BIND(IRI(?InvFunctProperty) as ?URIInvFunctProperty)
                  BIND(IRI(?FunctProperty) as ?URIFunctProperty)
                }
            """)
            sparql.query()

        except Exception as err:
            traceback.print_tb(err)

    def retrieve_functionalproperties_detectschemas(self):
        """
        Searches if alleged (inverse) functional properties have a schema in SPARQL endpoints (:label: LDS-(I)FP1).
        """
        try:
            sparql = SPARQLWrapper(self.master_endpoint)
            sparql.method = 'POST'
            sparql.setRequestMethod('postdirectly')
            sparql.setQuery("""
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX owl: <http://www.w3.org/2002/07/owl#>
                PREFIX void: <http://rdfs.org/ns/void#>
                PREFIX ends: <http://labs.mondeca.com/vocab/endpointStatus#>
                PREFIX same: <https://ns.inria.fr/same/same.owl#>
                PREFIX kg: <http://ns.inria.fr/corese/kgram/>

                INSERT {
                  GRAPH same:PropertiesNotDeferenced {
                    ?property a ?type1
                  }
                } WHERE {
                  GRAPH same:Properties {
                    ?property a ?type1
                    FILTER(?type1 IN (owl:InverseFunctionalProperty, owl:FunctionalProperty))
                  }
                  # Property not previously deferenced
                  FILTER NOT EXISTS { GRAPH kg:default { ?property a ?type2 } }
                }
            """)
            sparql.query()

            dic_datasets = LocalManipulation().get_datasets()
            print("Searching properties definition on endpoints.")
            # Multithreading for paging
            with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.map(self._retrieve_functionalproperties_detectschemas_pagination, dic_datasets)
        except Exception as err:
            traceback.print_tb(err)

    def _retrieve_functionalproperties_detectschemas_pagination(self, dic_datasets):
        try:
            sparql = SPARQLWrapper(self.master_endpoint)
            # may be improved, suboptimal
            for k, v in dic_datasets.items():
                try:
                    sparql.method = 'GET'
                    sparql.setReturnFormat(JSON)
                    sparql.setTimeout(self.timeout)
                    sparql.setQuery("""
                                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                                PREFIX owl: <http://www.w3.org/2002/07/owl#>
    
                                SELECT ?propertyCount WHERE {
                                  # owl:inverseOf
                                  SERVICE <%s> {
                                    SELECT (count(DISTINCT ?property) as ?propertyCount) WHERE {
                                      ?property a ?type ;
                                      ?p ?o
                                      FILTER (?type = owl:DatatypeProperty || ?type = rdf:Property || 
                                      ?type = owl:ObjectProperty || ?type = owl:InverseFunctionalProperty ||
                                      ?type = owl:FunctionalProperty)
                                      FILTER(?p = rdfs:label || ?p = rdfs:range || ?p = rdfs:domain)
                                    }
                                  }
                                }
                            """ % (str(v)))

                    json = sparql.query().convert()["results"]["bindings"]
                    print(json)
                    count = int([j["propertyCount"]["value"] for j in json][0])
                    for page in range(ceil(count / 10000)):
                        sparql.method = 'POST'
                        sparql.setRequestMethod('postdirectly')
                        sparql.setTimeout(self.timeout)
                        sparql.setQuery("""
                                    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                                    PREFIX owl: <http://www.w3.org/2002/07/owl#>
                                    PREFIX void: <http://rdfs.org/ns/void#>
                                    PREFIX ends: <http://labs.mondeca.com/vocab/endpointStatus#>
                                    PREFIX same: <https://ns.inria.fr/same/same.owl#>
    
                                    INSERT {
                                      GRAPH same:Properties {
                                        <%s> same:hasSchemaFor ?property
                                      }
                                    } WHERE {
                                      GRAPH same:PropertiesNotDeferenced {
                                        ?localProperty a ?type1
                                      }
    
                                      service <%s> {
                                        SELECT distinct ?property WHERE {
                                          ?property a ?type ;
                                          ?p ?o
                                          FILTER (?type = owl:DatatypeProperty || ?type = rdf:Property || 
                                          ?type = owl:ObjectProperty || ?type = owl:InverseFunctionalProperty ||
                                          ?type = owl:FunctionalProperty)
                                          FILTER(?p = rdfs:label || ?p = rdfs:range || ?p = rdfs:domain)
                                        } order by asc(?property) limit 10000 offset %s
                                      }
                                      FILTER(?property = ?localProperty)
                                    } 
                                """ % (k, str(v), str(page * 10000)))
                        sparql.query()
                except socket.error as err:
                    print(err)
        # include socket.error
        except Exception as err:
            traceback.print_tb(err)

    def _generate_query_pattern_functionalproperties_links1(self, iterator: int = 1, sparql_annotations: str = "",
                                                            dataset_options: str = "",
                                                            target_options="FILTER(!REGEX(str(?IRITarget), \"[^\\\\x00-\\\\x7F]\", \"i\"))"):
        """
        Generates the query used to retrieve (inverse) functional properties patterns (:label: (I)FP1).
        :param iterator: int, iteration of the algorithm.
        :param sparql_annotations: String, SPARQL Annotations of the Corese engine
        (https://ns.inria.fr/sparql-extension/event.html#event).
        :param dataset_options: String, Options on the available SPARQL endpoints.
        :param target_options: String, Options on the same:Target resources.
        :return: String, SPARQL query used to retrieve (inverse) functional properties patterns
        """
        query = """
                    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                    PREFIX owl: <http://www.w3.org/2002/07/owl#>
                    PREFIX void: <http://rdfs.org/ns/void#>
                    PREFIX ends: <http://labs.mondeca.com/vocab/endpointStatus#>
                    PREFIX same: <https://ns.inria.fr/same/same.owl#>
                    PREFIX kg: <http://ns.inria.fr/corese/kgram/>
                    
                    %s
                    INSERT {
                      GRAPH same:InverseFunctionalProperty_%s {
                        ?IRITarget ?P1 ?InverseFunctionalObject .
                      }
                      GRAPH same:FunctionalProperty_%s {
                        ?FunctionalSubject ?P2 ?IRITarget .
                      }
                      same:InverseFunctionalProperty_%s same:hasIteration %d .
                      same:FunctionalProperty_%s same:hasIteration %d
                    } WHERE {
                      GRAPH same:N {
                        ?dataset void:sparqlEndpoint ?endpoint ;
                        ends:status ?status .
                        ?status ends:statusIsAvailable true ;
                        %s
                      }
    
                      GRAPH same:Q%s  {
                        ?IRITarget a same:Target
                        %s
                      }
                      GRAPH kg:default {
                        ?IFP rdf:type|same:votingType owl:InverseFunctionalProperty .
                        ?FP rdf:type|same:votingType owl:FunctionalProperty
                      }
                      
                      # 2 Queries are better for memory management
                      OPTIONAL {
                      GRAPH kg:default {
                        ?IFP rdf:type|same:votingType owl:InverseFunctionalProperty .
                        SERVICE ?endpoint {
                        # Search inverse functional relations inferred with a known owl:InverseFunctionalProperty property
                          ?IRITarget ?P1 ?InverseFunctionalObject
                        }
                        FILTER(?P1 = ?IFP)
                      }
                      }
    
                      OPTIONAL {
                      GRAPH kg:default {
                        ?FP rdf:type|same:votingType owl:FunctionalProperty
                        SERVICE ?endpoint {
                        # Search inverse functional relations inferred with a known owl:InverseFunctionalProperty property
                          ?FunctionalSubject ?P2 ?IRITarget
                        }
                        FILTER(?P2 = ?FP)
                      }
                      }
                    }
                """ % (sparql_annotations, str(iterator), str(iterator), str(iterator), iterator, str(iterator), iterator,
                       dataset_options, str(iterator - 1), target_options)

        return query

    def _generate_queries_pattern_functionalproperties_links2(self, iterator: int = 1, sparql_events: str = "",
                                                              dataset_options: str = "", *_):
        """
        Generates the queries to compute new same:Target resources and owl:sameAs relationships with (inverse)
        functional properties patterns (:label: (I)FP2).
        :param iterator: int, iteration of the algorithm.
        :param sparql_events: String, SPARQL Annotations of the Corese engine
        (https://ns.inria.fr/sparql-extension/event.html#event).
        :param dataset_options: String, Options on the available SPARQL endpoints.
        :param _: Unused parameter, it is here for compatibilities with other functions.
        :return: String, SPARQL query used to retrieve (inverse) functional properties patterns
        """
        query_fp = """
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX owl: <http://www.w3.org/2002/07/owl#>
                PREFIX void: <http://rdfs.org/ns/void#>
                PREFIX ends: <http://labs.mondeca.com/vocab/endpointStatus#>
                PREFIX same: <https://ns.inria.fr/same/same.owl#>
                %s
                INSERT {
                  GRAPH ?ngraph {
                    ?URITargetI1 owl:sameAs ?URITargetI2 .
                    ?URITargetI2 owl:sameAs ?URITargetI1 .
                    ?URITargetF1 owl:sameAs ?URITargetF2 .
                    ?URITargetF2 owl:sameAs ?URITargetF1 .
                  }
                  ?ngraph same:hasIteration %d
                  
                  ?execution a fno:Execution, prov:Activity ;
                  fno:executes same:IFP2 ;
                  dcterms:date ?date .

                  ?ngraph prov:wasGeneratedBy ?execution .
                  
                  GRAPH same:Q%s {
                    ?URITargetI2 a same:Target ;
                    void:inDataset ?dataset ;
                    same:hasNamespace ?nstargeti2 ;
                    same:hasAuthority ?auti2 ;
                    same:hasValueWithNoScheme ?noschemetargeti2 .
                    ?URITargetF2 a same:Target ;
                    void:inDataset ?dataset ;
                    same:hasNamespace ?nstargetf2 ;
                    same:hasAuthority ?autf2 ;
                    same:hasValueWithNoScheme ?noschemetargetf2 .
                  }
                  GRAPH same:InverseFunctionalProperty_%s {
                    ?URITargetI2 ?IFP ?InverseFunctionalObject
                  }
                  GRAPH same:FunctionalProperty_%s {
                    ?FunctionalSubject ?FP ?URITargetF2
                  }
                  same:Q%s same:hasIteration %d .
                  same:InverseFunctionalProperty_%s same:hasIteration %d .
                  same:FunctionalProperty_%s same:hasIteration %d
                } WHERE {
                  GRAPH same:N {
                    ?dataset void:sparqlEndpoint ?endpoint ;
                    ends:status ?status .
                    ?status ends:statusIsAvailable true ;
                    %s
                  }
                  OPTIONAL {
                    GRAPH same:InverseFunctionalProperty_%s {
                      ?URITargetI1 ?IFP ?InverseFunctionalObject
                    }
                    SERVICE ?endpoint {
                      ?URITargetI2 ?IFP ?InverseFunctionalObject
                      FILTER(!isBlank(?URITargetI2))
                    }
                    FILTER(!EXISTS { ?URITargetI2 a same:Target })
                    FILTER(!EXISTS { ?URITargetI2 a same:Rotten })
                  }
                  OPTIONAL {
                    GRAPH same:FunctionalProperty_%s {
                      ?FunctionalSubject ?FP ?URITargetF1
                    }
                    SERVICE ?endpoint {
                      ?FunctionalSubject ?FP ?URITargetF2
                      FILTER(!isBlank(?URITargetF2))
                    }
                    FILTER(!EXISTS { ?URITargetF2 a same:Target })
                    FILTER(!EXISTS { ?URITargetF2 a same:Rotten })
                  }
                  BIND(IRI(concat(str(?endpoint), '#', %s, 'IFP2')) as ?ngraph)
                  BIND(REPLACE(STR(?URITargetI2), "(#|/)[^#/]*$", "$1") as ?nstargeti2)
                  BIND(REPLACE(STR(?URITargetI2), ".+://(.*?)/.*", "$1") as ?auttargeti2)
                  BIND(REPLACE(STR(?URITargetI2), ".+://(.*)", "$1") as ?noschemetargeti2)
                  BIND(REPLACE(STR(?URITargetF2), "(#|/)[^#/]*$", "$1") as ?nstargetf2)
                  BIND(REPLACE(STR(?URITargetF2), ".+://(.*?)/.*", "$1") as ?auttargetf2)
                  BIND(REPLACE(STR(?URITargetF2), ".+://(.*)", "$1") as ?noschemetargetf2)
                  FILTER(?URITargetI1 != ?URITargetI2)
                  FILTER(?URITargetF1 != ?URITargetF2)
                }
            """ % (sparql_events, iterator, str(iterator), str(iterator), str(iterator), str(iterator), iterator,
                   str(iterator), iterator, str(iterator), iterator, str(iterator), dataset_options, str(iterator),
                   str(iterator))

        query_ifp = """
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX owl: <http://www.w3.org/2002/07/owl#>
                PREFIX void: <http://rdfs.org/ns/void#>
                PREFIX ends: <http://labs.mondeca.com/vocab/endpointStatus#>
                PREFIX same: <https://ns.inria.fr/same/same.owl#>
                %s
                INSERT {
                  GRAPH ?ngraph {
                    ?URITargetI1 owl:sameAs ?URITargetI2 .
                    ?URITargetI2 owl:sameAs ?URITargetI1 .
                    ?URITargetF1 owl:sameAs ?URITargetF2 .
                    ?URITargetF2 owl:sameAs ?URITargetF1 .
                  }
                  ?ngraph same:hasIteration %d
                  GRAPH same:Q%s {
                    ?URITargetI2 a same:Target ;
                    void:inDataset ?dataset ;
                    same:hasNamespace ?nstargeti2 ;
                    same:hasAuthority ?auti2 ;
                    same:hasValueWithNoScheme ?noschemetargeti2 .
                    ?URITargetF2 a same:Target ;
                    void:inDataset ?dataset ;
                    same:hasNamespace ?nstargetf2 ;
                    same:hasAuthority ?autf2 ;
                    same:hasValueWithNoScheme ?noschemetargetf2 .
                  }
                  GRAPH same:InverseFunctionalProperty_%s {
                    ?URITargetI2 ?IFP ?InverseFunctionalObject
                  }
                  GRAPH same:FunctionalProperty_%s {
                    ?FunctionalSubject ?FP ?URITargetF2
                  }
                  same:Q%s same:hasIteration %d .
                  same:InverseFunctionalProperty_%s same:hasIteration %d .
                  same:FunctionalProperty_%s same:hasIteration %d
                } WHERE {
                  GRAPH same:N {
                    ?dataset void:sparqlEndpoint ?endpoint ;
                    ends:status ?status .
                    ?status ends:statusIsAvailable true ;
                    %s
                  }
                  OPTIONAL {
                    GRAPH same:InverseFunctionalProperty_%s {
                      ?URITargetI1 ?IFP ?InverseFunctionalObject
                    }
                    SERVICE ?endpoint {
                      ?URITargetI2 ?IFP ?InverseFunctionalObject
                      FILTER(!isBlank(?URITargetI2))
                    }
                    FILTER(!EXISTS { ?URITargetI2 a same:Target })
                    FILTER(!EXISTS { ?URITargetI2 a same:Rotten })
                  }
                  OPTIONAL {
                    GRAPH same:FunctionalProperty_%s {
                      ?FunctionalSubject ?FP ?URITargetF1
                    }
                    SERVICE ?endpoint {
                      ?FunctionalSubject ?FP ?URITargetF2
                      FILTER(!isBlank(?URITargetF2))
                    }
                    FILTER(!EXISTS { ?URITargetF2 a same:Target })
                    FILTER(!EXISTS { ?URITargetF2 a same:Rotten })
                  }
                  BIND(IRI(concat(str(?endpoint), '#', %s)) as ?ngraph)
                  BIND(REPLACE(STR(?URITargetI2), "(#|/)[^#/]*$", "$1") as ?nstargeti2)
                  BIND(REPLACE(STR(?URITargetI2), ".+://(.*?)/.*", "$1") as ?auttargeti2)
                  BIND(REPLACE(STR(?URITargetI2), ".+://(.*)", "$1") as ?noschemetargeti2)
                  BIND(REPLACE(STR(?URITargetF2), "(#|/)[^#/]*$", "$1") as ?nstargetf2)
                  BIND(REPLACE(STR(?URITargetF2), ".+://(.*?)/.*", "$1") as ?auttargetf2)
                  BIND(REPLACE(STR(?URITargetF2), ".+://(.*)", "$1") as ?noschemetargetf2)
                  FILTER(?URITargetI1 != ?URITargetI2)
                  FILTER(?URITargetF1 != ?URITargetF2)
                }
            """ % (sparql_events, iterator, str(iterator), str(iterator), str(iterator), str(iterator), iterator,
                    str(iterator), iterator, str(iterator), iterator, str(iterator), dataset_options, str(iterator),
                   str(iterator))
        return query_fp, query_ifp


class ErrorDetection(object):
    def __init__(self):
        self.master_endpoint = Config.master_endpoint

    def rotten_sameas(self, iterator: int = 1):
        """
        Identifies 'rotten' owl:sameAs relations by checking that a resource does not lead to an another resource with
        the same authority at distinct iterations, stores the URIs as same:Rotten then deletes the relation
        (:label: R1).
        :param iterator: int, iteration of the algorithm.
        """
        try:
            sparql = SPARQLWrapper(self.master_endpoint)
            sparql.method = 'POST'
            sparql.setRequestMethod('postdirectly')
            sparql.setQuery("""
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX owl: <http://www.w3.org/2002/07/owl#>
                PREFIX void: <http://rdfs.org/ns/void#>
                PREFIX same: <https://ns.inria.fr/same/same.owl#>
                
                INSERT {
                  GRAPH same:Q-1 {
                    ?Rotten a same:Rotten ;
                    void:inDataset ?DatasetRotten ;
                    same:hasNamespace ?nsrotten ;
                    same:hasAuthority ?autrotten
                  }
                } WHERE {
                    GRAPH same:Q%s  {
                      ?IRITarget a same:Target ;
                      same:hasNamespace ?nstarget ;
                      same:hasAuthority ?auttarget ;
                      same:hasValueWithNoScheme ?noschemetarget
                    }
                    ?x (owl:sameAs|^owl:sameAs)+ ?IRITarget .
                    ?x same:hasNamespace ?nsx .
                    ?x same:hasAuthority ?autx .
                    ?x same:hasValueWithNoScheme ?noschemex
                    FILTER(?noschemex != ?noschemetarget && ?autx = ?auttarget)
                    # Checks that it is an indirect relationship
                    FILTER(!EXISTS { ?x owl:sameAs ?IRITarget } )
                    FILTER(!EXISTS { GRAPH ?g { ?x a same:Target }
                                   ?g same:hasIteration ?it
                                   FILTER(xsd:integer(?it) != xsd:integer(%s))
                                  })
                    
                    # Retrieve information related to Rotten links
                    ?Rotten owl:sameAs ?IRITarget ;
                    same:hasNamespace ?nsrotten ;
                    same:hasAuthority ?autrotten
                    # OPTIONAL if ?x in Q0
                    OPTIONAL { ?Rotten void:inDataset ?DatasetRotten }
                }
            """ % (str(iterator), str(iterator - 1)))
            sparql.query()

            self.rotten_sameas_cleanup()

        except Exception as err:
            traceback.print_tb(err)

    def rotten_sameas2(self, iterator: int = 1):
        """
        Identifies 'rotten' owl:sameAs relations by checking that a resource does not lead to an another resource with
        the same authority at the same iteration, stores the URIs as same:Rotten then deletes the relation (:label: R2).
        :param iterator: int, iteration of the algorithm.
        """
        try:
            sparql = SPARQLWrapper(self.master_endpoint)
            sparql.method = 'POST'
            sparql.setRequestMethod('postdirectly')
            sparql.setQuery("""
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX owl: <http://www.w3.org/2002/07/owl#>
                PREFIX void: <http://rdfs.org/ns/void#>
                PREFIX same: <https://ns.inria.fr/same/same.owl#>
                
                INSERT {
                  GRAPH same:Q-1 {
                    ?y a same:Rotten ;
                    void:inDataset ?DatasetY ;
                    same:hasNamespace ?nsy ;
                    same:hasAuthority ?auty
                  }
                } WHERE {
                    ?x (owl:sameAs|^owl:sameAs)+ ?y .
                    ?x same:hasNamespace ?nsx .
                    ?x same:hasAuthority ?autx .
                    ?x same:hasValueWithNoScheme ?noschemex .
                    ?y same:hasNamespace ?nsy .
                    ?y same:hasAuthority ?auty .
                    ?y same:hasValueWithNoScheme ?noschemey .
                    OPTIONAL { ?y void:inDataset ?DatasetY }
                    FILTER(?noschemex != ?noschemey && ?autx = ?auty)
                    FILTER(!EXISTS { ?x owl:sameAs ?y } )
                    FILTER(!EXISTS { GRAPH ?g1 { ?x a same:Target }
                                     GRAPH ?g2 { ?y a same:Target }
                                   ?g1 same:hasIteration ?it1 .
                                   ?g2 same:hasIteration ?it2 .
                                   FILTER(xsd:integer(?it1) != xsd:integer(%s)
                                   && xsd:integer(?it2) != xsd:integer(%s)
                                   && xsd:integer(?it1) = xsd:integer(?it2))
                                  })
                }
            """ % (str(iterator), str(iterator)))
            sparql.query()

            self.rotten_sameas_cleanup()

        except Exception as err:
            traceback.print_tb(err)

    def rotten_sameas_cleanup(self):
        """
        Removes same:Target resources identified as same:Rotten, and deletes their incoming ond outgoing owl:sameAs
        relationships (:label: CR1).
        """
        try:
            sparql = SPARQLWrapper(self.master_endpoint)
            sparql.method = 'POST'
            sparql.setRequestMethod('postdirectly')
            sparql.setQuery("""
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX owl: <http://www.w3.org/2002/07/owl#>
                PREFIX void: <http://rdfs.org/ns/void#>
                PREFIX same: <https://ns.inria.fr/same/same.owl#>

                DELETE {
                  GRAPH ?g2 {
                    ?x a same:Target ;
                    void:inDataset ?dataset ;
                    same:hasNamespace ?nsx ;
                    same:hasAuthority ?autx
                  }
                } WHERE {
                  GRAPH same:Q-1  {
                    ?Rotten a same:Rotten
                  }
                  ?x (owl:sameAs|^owl:sameAs) ?Rotten .
                  ?x same:hasNamespace ?nsx .
                  ?x same:hasAuthority ?autx
                  # OPTIONAL if ?x in Q0
                  OPTIONAL { ?x void:inDataset ?dataset }
                  GRAPH ?g1 {
                    ?Rotten a same:Target
                  }
                  ?g1 same:hasIteration ?it1
                  BIND(IRI(CONCAT(STR(same:),"Q", str(?it1+1))) as ?g2)
                  FILTER(!EXISTS {?x owl:sameAs ?y .
                                  ?y a ?yType
                                  FILTER(?yType != same:Rotten) })
                }
            """)
            sparql.query()

            sparql.setQuery("""
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX owl: <http://www.w3.org/2002/07/owl#>
                PREFIX void: <http://rdfs.org/ns/void#>
                PREFIX same: <https://ns.inria.fr/same/same.owl#>

                DELETE {
                  ?x owl:sameAs ?Rotten .
                  ?Rotten owl:sameAs ?x
                  GRAPH ?g1 {
                    ?Rotten a same:Target ;
                    void:inDataset ?DatasetRotten ;
                    same:hasNamespace ?nsrotten ;
                    same:hasAuthority ?autrotten
                  }
                } WHERE {
                  GRAPH same:Q-1  {
                    ?Rotten a same:Rotten ;
                    void:inDataset ?DatasetRotten ;
                    same:hasNamespace ?nsrotten ;
                    same:hasAuthority ?autrotten
                  }
                  GRAPH ?g1 {
                    ?Rotten a same:Target
                  }
                  ?x (owl:sameAs|^owl:sameAs) ?Rotten
                }
            """)
            sparql.query()

            sparql.setQuery("""
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX owl: <http://www.w3.org/2002/07/owl#>
                PREFIX void: <http://rdfs.org/ns/void#>
                PREFIX same: <https://ns.inria.fr/same/same.owl#>
                PREFIX kg: <http://ns.inria.fr/corese/kgram/>

                DELETE {
                  ?Rotten ?IFP ?InverseFunctionalObject .
                  ?x ?IFP ?InverseFunctionalObject .
                  ?FunctionalSubject ?FP ?Rotten .
                  ?FunctionalSubject ?FP ?y
                } WHERE {
                  GRAPH same:Q-1  {
                    ?Rotten a same:Rotten
                  }
                  OPTIONAL {
                    ?Rotten ?IFP ?InverseFunctionalObject .
                    ?x ?IFP ?InverseFunctionalObject
                    FILTER( EXISTS { GRAPH kg:default { ?IFP rdf:type|same:votingType owl:InverseFunctionalObject } } )
                  }
                  OPTIONAL {
                    ?FunctionalSubject ?FP ?Rotten .
                    ?FunctionalSubject ?FP ?y
                    FILTER( EXISTS { GRAPH kg:default { ?FP rdf:type|same:votingType owl:FunctionalObject } } )
                  }
                }
            """)
            sparql.query()

        except Exception as err:
            traceback.print_tb(err)
