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
    setup.populate_lodcloud()
    # setup.populate_void_rkbexplorer()
    setup.populate_umakata()
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
    if Config.is_corese_engine:
        monitoring.handle_values_clause()
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
        print(len(resources_list))
        print(resources_list)
        endpoint_exploration.retrieve_sameas(iteration)
        if Config.FUNC_PROP:
            endpoint_exploration.retrieve_functionalproperties_links1(iteration)
            endpoint_exploration.retrieve_functionalproperties_links2(iteration)
        error_detection.rotten_sameas(iteration)
        error_detection.rotten_sameas2(iteration)
        iteration += 1
        # Polling
        resources_list = local_manipulation.get_targets(iteration)
    error_detection.rotten_sameas2(iteration)

    print("--- %s seconds ---" % (time.time() - start_time))
