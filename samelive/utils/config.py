import os


class Config(object):
    project_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    # Timeout used if an endpoint does not answer (expensive query) - no HTTP status code.
    # Timeout is currently only used to retrieve (inverse) functional properties.
    timeout = 200

    # Enables optimizations of the Corese engine (bindings with the clause VALUES)
    is_corese_engine = True

    # URL of the triplestore on which the UPDATE queries are performed.
    master_endpoint = "http://localhost:8082/sparql"

    # Set to True to process owl:InverseFunctionalProperty and owl:FunctionalProperty
    FUNC_PROP = True


    # Seed URIs of the algorithm to populate in same:Q0
    resources_list = ["http://dbpedia.org/resource/Barack_Obama"]

    # Additional endpoints to populate in same:N
    # key (Dataset URI) : endpoint (SPARQL endpoint URL)
    endpoints_dict = {}

    slave_endpoints = ["http://localhost:8083/sparql",
                           "http://localhost:8084/sparql"]
