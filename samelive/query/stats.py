import json
import traceback
import requests
from math import ceil
import concurrent.futures

from samelive.utils.config import Config
from samelive.utils.helper import Helper

import tqdm
from rdflib import Graph, ConjunctiveGraph
from SPARQLWrapper import SPARQLWrapper, JSON, N3, XML


class Statistics(object):
    def __init__(self):
        self.master_endpoint = Config.master_endpoint

    def compute_stats(self):
        try:
            self.nb_voted_functional_properties()
            self.nb_incorrect_functional_properties()
            self.nb_not_deferenced_properties()
            self.nb_not_loaded_rdf_document()
            self.nb_loaded_rdf_document()

        except Exception as err:
            traceback.print_tb(err)

    # Definition
    # same:ActivityReport
    # same:totalVotedAsInverseFunctionalProperty
    def nb_voted_functional_properties(self):
        """
        Computes the number of extracted properties and not deferenced voted as (inverse) functional properties.
        """
        try:
            sparql = SPARQLWrapper(self.master_endpoint)
            sparql.method = 'POST'
            sparql.setRequestMethod('postdirectly')
            sparql.setQuery("""
                PREFIX same: <https://ns.inria.fr/same/same.owl#>
                PREFIX kg: <http://ns.inria.fr/corese/kgram/>
                
                INSERT {
                  GRAPH same:Statistics {
                    same:ActivityReport same:totalVotedAsInverseFunctionalProperties ?nbx
                  }
                } WHERE {
                {
                  select (count(distinct ?x) as ?nbx) where {
                    GRAPH kg:default {
                      ?x same:votingType owl:InverseFunctionalProperty
                    }
                  }
                }
                }
            """)
            sparql.query()

            sparql.setQuery("""
                PREFIX same: <https://ns.inria.fr/same/same.owl#>
                PREFIX kg: <http://ns.inria.fr/corese/kgram/>
                
                INSERT {
                  GRAPH same:Statistics {
                    same:ActivityReport same:totalVotedAsFunctionalProperties ?nbx
                  }
                } WHERE {
                {
                  select (count(distinct ?x) as ?nbx) where {
                    GRAPH kg:default {
                      ?x same:votingType owl:FunctionalProperty
                    }
                  }
                }
                }
            """)
            sparql.query()

        except Exception as err:
            traceback.print_tb(err)

    def nb_incorrect_functional_properties(self):
        """
        Computes the number of incorrect (inverse) functional properties defined as such after deferencing them
        """
        try:
            sparql = SPARQLWrapper(self.master_endpoint)
            sparql.method = 'POST'
            sparql.setRequestMethod('postdirectly')
            sparql.setQuery("""
                PREFIX same: <https://ns.inria.fr/same/same.owl#>
                PREFIX kg: <http://ns.inria.fr/corese/kgram/>

                INSERT {
                  GRAPH same:Statistics {
                    same:ActivityReport same:totalIncorrectFunctionalProperties ?nbx
                  }
                } WHERE {
                {
                  select (count(distinct ?x) as ?nbx) where {
                    GRAPH same:Properties {
                      ?x a owl:FunctionalProperty
                    }
                    FILTER (EXISTS {GRAPH kg:default { ?x a ?y }} )
                    FILTER (!EXISTS {GRAPH kg:default { ?x a owl:FunctionalProperty }} )
                  }
                }
                }
            """)
            sparql.query()

            sparql.setQuery("""
                PREFIX same: <https://ns.inria.fr/same/same.owl#>
                PREFIX kg: <http://ns.inria.fr/corese/kgram/>

                INSERT {
                  GRAPH same:Statistics {
                    same:ActivityReport same:totalIncorrectInverseFunctionalProperties ?nbx
                  }
                } WHERE {
                {
                  select (count(distinct ?x) as ?nbx) where {
                    GRAPH same:Properties {
                      ?x a owl:InverseFunctionalProperty
                    }
                    FILTER (EXISTS {GRAPH kg:default { ?x a ?y }} )
                    FILTER (!EXISTS {GRAPH kg:default { ?x a owl:InverseFunctionalProperty }} )
                  }
                }
                }
            """)
            sparql.query()

        except Exception as err:
            traceback.print_tb(err)

    def nb_not_deferenced_properties(self):
        """
        Computes the number of not deferenced (inverse) functional properties.
        """
        try:
            sparql = SPARQLWrapper(self.master_endpoint)
            sparql.method = 'POST'
            sparql.setRequestMethod('postdirectly')
            sparql.setQuery("""
                PREFIX same: <https://ns.inria.fr/same/same.owl#>
                PREFIX kg: <http://ns.inria.fr/corese/kgram/>

                INSERT {
                  GRAPH same:Statistics {
                    same:ActivityReport same:totalNotDeferencedProperties ?nbx
                  }
                } WHERE {
                {
                  select (count(distinct ?x) as ?nbx) where {
                    GRAPH same:PropertiesNotDeferenced {
                      ?x a ?type
                    }
                  }
                }
                }
            """)
            sparql.query()

        except Exception as err:
            traceback.print_tb(err)

    def nb_not_loaded_rdf_document(self):
        """
        Computes the number of RDF documents that could not be loaded with a LOAD clause.
        """
        try:
            sparql = SPARQLWrapper(self.master_endpoint)
            sparql.method = 'POST'
            sparql.setRequestMethod('postdirectly')
            sparql.setQuery("""
                PREFIX same: <https://ns.inria.fr/same/same.owl#>
                PREFIX kg: <http://ns.inria.fr/corese/kgram/>

                INSERT {
                  GRAPH same:Statistics {
                    same:ActivityReport same:totalNotLoadedRDFDocuments ?nbns
                  }
                } WHERE {
                {
                  select (count(distinct ?ns) as ?nbns) where {
                    GRAPH same:Properties {
                      ?property same:hasNamespace ?ns
                    }
                    GRAPH kg:default {
                      ?resource a ?type
                    }
                    FILTER(STRSTARTS(STR(?resource), ?ns))
                  }
                }
                }
            """)
            sparql.query()

        except Exception as err:
            traceback.print_tb(err)

    def nb_loaded_rdf_document(self):
        """
        Computes the number of RDF documents that could be loaded with a LOAD clause.
        """
        try:
            sparql = SPARQLWrapper(self.master_endpoint)
            sparql.method = 'POST'
            sparql.setRequestMethod('postdirectly')
            sparql.setQuery("""
                PREFIX same: <https://ns.inria.fr/same/same.owl#>
                PREFIX kg: <http://ns.inria.fr/corese/kgram/>

                INSERT {
                  GRAPH same:Statistics {
                    same:ActivityReport same:totalLoadedRDFDocuments ?nbdoc
                  }
                } WHERE {
                {
                  select (count(distinct ?ns) as ?nbns) where {
                    GRAPH same:Properties {
                      ?property same:hasNamespace ?ns
                    }
                    FILTER(STRSTARTS(STR(?property), ?ns))
                  }
                }
                  GRAPH same:Statistics {
                    same:ActivityReport same:totalNotLoadedRDFDocuments ?nbnotloaded
                  }
                  BIND((?nbns - ?nbnotloaded) as ?nbdoc)
                }
            """)
            sparql.query()

        except Exception as err:
            traceback.print_tb(err)
