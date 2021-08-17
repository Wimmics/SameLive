import json
import traceback
import requests
import socket
from math import ceil
import concurrent.futures

from samelive.utils.config import Config
from samelive.utils.helper import Helper

from rdflib import Graph, ConjunctiveGraph
from SPARQLWrapper import SPARQLWrapper, JSON, N3, XML


class Monitoring(object):
    def __init__(self):
        self.master_endpoint = Config.master_endpoint

    def endpoints_availability(self):
        """
        Checks the availability of endpoints in same:N and store this information in the same named graph
        (:label: A1).
        """
        try:
            sparql = SPARQLWrapper(self.master_endpoint)
            sparql.method = 'POST'
            sparql.setRequestMethod('postdirectly')
            sparql.setQuery("""
                PREFIX same: <https://ns.inria.fr/same/same.owl#>
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX void: <http://rdfs.org/ns/void#>
                PREFIX ends: <http://labs.mondeca.com/vocab/endpointStatus#>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                PREFIX dcterms: <http://purl.org/dc/terms/>
                WITH same:N
                INSERT {
                  ?dataset ends:status ?status .
                  ?status a ends:EndpointStatus ;
                  ends:statusIsAvailable ?available ;
                  dcterms:date ?date
                } WHERE {
                  ?dataset void:sparqlEndpoint ?endpoint
                  FILTER NOT EXISTS {
                    ?dataset ends:status ?status .
                    ?status ends:statusIsAvailable ?available
                  }

                  # Replace to comply with RFC 3986
                  BIND(URI(CONCAT(STR(REPLACE(STR(?dataset), ".dataset", "")), ".status")) as ?status)

                  OPTIONAL {
                    SERVICE ?endpoint {
                      SELECT ?x WHERE {
                        ?x ?p ?y
                      } LIMIT 1
                    }
                  }
                  BIND(IF(BOUND(?x), true, false)  as ?available)
                  BIND(xsd:dateTime(NOW()) AS ?date)
                }
            """)
            sparql.query()

        except Exception as err:
            traceback.print_tb(err)

    def handle_values_clause(self):
        """
        Identifies if the available endpoints support the VALUES clause
        (https://www.w3.org/TR/sparql11-query/#sparqlAlgebraFinalValues).
        """
        try:
            sparql = SPARQLWrapper(self.master_endpoint)
            sparql.method = 'POST'
            sparql.setRequestMethod('postdirectly')
            sparql.setQuery("""
                PREFIX same: <https://ns.inria.fr/same/same.owl#>
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX void: <http://rdfs.org/ns/void#>
                PREFIX ends: <http://labs.mondeca.com/vocab/endpointStatus#>
                INSERT {
                  GRAPH same:N {
                    ?status same:valuesIsAvailable ?isValues
                  }
                } WHERE {
                  GRAPH same:N {
                    ?dataset void:sparqlEndpoint ?endpoint ;
                    ends:status ?status .
                    ?status ends:statusIsAvailable true
                  }
                  
                  OPTIONAL {
                    SERVICE ?endpoint {
                      SELECT ?x WHERE {
                        VALUES ?dummy { "dummy" }
                        ?x a ?y
                      } LIMIT 1
                    }
                  }
                  BIND(IF(BOUND(?x), true, false)  as ?isValues)
                }
            """)
            sparql.query()

        except Exception as err:
            traceback.print_tb(err)

    def handle_non_ascii_character(self):
        """
        Identifies if the available endpoints support or not non-ASCII characters.
        """
        try:
            sparql = SPARQLWrapper(self.master_endpoint)
            sparql.method = 'POST'
            sparql.setRequestMethod('postdirectly')
            sparql.setQuery("""
                PREFIX same: <https://ns.inria.fr/same/same.owl#>
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX void: <http://rdfs.org/ns/void#>
                PREFIX ends: <http://labs.mondeca.com/vocab/endpointStatus#>
                INSERT {
                  GRAPH same:N {
                    ?status same:supportsNonASCIICharacters ?supportsNonASCIICharacters
                  }
                } WHERE {
                  GRAPH same:N {
                    ?dataset void:sparqlEndpoint ?endpoint ;
                    ends:status ?status .
                    ?status ends:statusIsAvailable true
                  }
                  
                  OPTIONAL {
                    SERVICE ?endpoint {
                      SELECT ?x WHERE {
                        ?x a ?y
                        OPTIONAL { ?x1 ?p1 "あ" } 
                      } LIMIT 1
                    }
                  }
                  BIND(IF(BOUND(?x), true, false)  as ?supportsNonASCIICharacters)
                }
            """)
            sparql.query()

        except Exception as err:
            traceback.print_tb(err)

    def has_limit(self):
        """
        Computes the limit number of results returned by the available endpoints.
        """
        try:
            sparql = SPARQLWrapper(self.master_endpoint)
            sparql.method = 'POST'
            sparql.setRequestMethod('postdirectly')
            sparql.setQuery("""
                PREFIX same: <https://ns.inria.fr/same/same.owl#>
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX void: <http://rdfs.org/ns/void#>
                PREFIX ends: <http://labs.mondeca.com/vocab/endpointStatus#>
                INSERT {
                  GRAPH same:N {
                    ?status same:hasLimit ?limit
                  }
                } WHERE {
                  {
                  select (count(?x) as ?limit) ?status where {
                    GRAPH same:N {
                      ?dataset void:sparqlEndpoint ?endpoint ;
                      ends:status ?status .
                      ?status ends:statusIsAvailable true
                    }
                    SERVICE ?endpoint {
                      SELECT ?x WHERE {
                        ?x ?p ?y
                      }
                    }
                  }
                  }
                }
            """)
            sparql.query()

        except Exception as err:
            traceback.print_tb(err)
