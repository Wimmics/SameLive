import time
import concurrent.futures
from functools import partial
import traceback
from SPARQLWrapper import SPARQLWrapper, JSON, N3, XML

from samelive.utils.config import Config
from samelive.query.querymanager import EndpointExploration, LocalManipulation, ErrorDetection, Setup
from samelive.query.monitoring import Monitoring

setup = Setup()
endpoint_exploration = EndpointExploration()
local_manipulation = LocalManipulation()
error_detection = ErrorDetection()
monitoring = Monitoring()

if __name__ == '__main__':
    setup.setup_vocabulary()
    # setup.populate_void_rkbexplorer()
    setup.populate_lodcloud()
    setup.populate_umakata()
    setup.populate_linkedwiki()
    setup.populate_datahub()
    setup.cleanup_datasets()

    try:
        sparql = SPARQLWrapper(Config.master_endpoint)
        sparql.method = 'POST'
        sparql.setRequestMethod('postdirectly')
        sparql.setQuery("""
            PREFIX same: <https://ns.inria.fr/same/same.owl#>
            PREFIX owl: <http://www.w3.org/2002/07/owl#>
            INSERT {
              GRAPH same:Q0 {
                ?entity a same:Target ;
                same:hasNamespace ?nstarget ;
                same:hasAuthority ?auttarget ;
                same:hasValueWithNoScheme ?noschemetarget
              }
              same:Q0 same:hasIteration 0
            } WHERE {
              SERVICE <https://covidontheweb.inria.fr/sparql> {
                select distinct ?entity where {
                  ?annot oa:hasBody ?entity;
                    # Paper title: "COVID-19: what has been learned and to be learned about 
                    # the novel coronavirus disease"
                    <http://schema.org/about> <http://ns.inria.fr/covid19/0eadf5a901c0d89fad2c202990056556be103e12> .
                }
              }
              BIND(REPLACE(STR(?entity), "(#|/)[^#/]*$", "$1") as ?nstarget)
              BIND(REPLACE(STR(?entity), ".+://(.*)", "$1") as ?noschemetarget)
              BIND(REPLACE(STR(?entity), ".+://(.*?)/.*", "$1") as ?auttarget)
            }
        """)
        sparql.query()
    except Exception as err:
        traceback.print_tb(err)
    monitoring.endpoints_availability()
    # Optimizations with the Corese engine
    if Config.IS_CORESE_ENGINE:
        monitoring.handle_values_clause()
    if Config.NON_ASCII_CHARACTERS_HANDLING:
        monitoring.handle_non_ascii_character()
    iteration = 1
    resources_list = local_manipulation.get_targets(iteration)
    if Config.FUNC_PROP:
        endpoint_exploration.retrieve_functionalproperties_schemas()
        setup.load_vocabularies_functionalproperties()
        endpoint_exploration.retrieve_functionalproperties_detectschemas()
        local_manipulation.voting_functionalproperties()
    start_time = time.time()
    # While there are same:Target in the current iteration named graph
    while len(resources_list) != 0:
        print("Iteration: " + str(iteration))
        print("Number of resources of type same:Target in the current iteration: " + str(len(resources_list)))
        print("Resources of type same:Target used in the current iteration:")
        print(resources_list)
        # :label: S1
        endpoint_exploration.optimize_remote_queries(endpoint_exploration._generate_query_pattern_sameas, iteration)
        if Config.FUNC_PROP:
            # :label: (I)FP1 and (I)FP2
            endpoint_exploration.optimize_remote_queries(
                endpoint_exploration._generate_query_pattern_functionalproperties_links1, iteration)
            endpoint_exploration.optimize_remote_queries(
                endpoint_exploration._generate_queries_pattern_functionalproperties_links2, iteration)
        error_detection.rotten_sameas(iteration)
        error_detection.rotten_sameas2(iteration)
        iteration += 1
        # Polling
        resources_list = local_manipulation.get_targets(iteration)
    error_detection.rotten_sameas2(iteration)

    print("--- %s seconds ---" % (time.time() - start_time))
