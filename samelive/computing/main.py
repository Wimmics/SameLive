import time
import concurrent.futures
from functools import partial

from samelive.utils.config import Config
from samelive.query.querymanager import EndpointExploration, LocalManipulation, ErrorDetection, Setup
from samelive.query.monitoring import Monitoring

setup = Setup()
endpoint_exploration = EndpointExploration()
local_manipulation = LocalManipulation()
error_detection = ErrorDetection()
monitoring = Monitoring()

# Traces on the configurations options
print("Handles (inverse) functional properties: " + str(Config.FUNC_PROP))
if Config.FUNC_PROP:
    print("Timeout used to retrieve (inverse) functional properties: " + str(Config.timeout))

if __name__ == '__main__':
    setup.setup_vocabulary()
    # :label: N1 to N5
    # setup.populate_void_rkbexplorer()
    setup.populate_lodcloud()
    setup.populate_umakata()
    setup.populate_linkedwiki()
    setup.populate_datahub()
    # :label: CN1
    setup.cleanup_datasets()
    # :label: P1
    setup.populate(Config.resources_list, Config.endpoints_dict)
    # :label: A1
    monitoring.endpoints_availability()
    # Optimizations with the Corese engine
    if Config.is_corese_engine:
        monitoring.handle_values_clause()
    iteration = 1
    # :label: T1
    resources_list = local_manipulation.get_targets(iteration)
    if Config.FUNC_PROP:
        # Respectively, :label: G-(I)FP1, LDD-(I)FP1, LDS-(I)FP1 and V-(I)FP1
        endpoint_exploration.retrieve_functionalproperties_schemas()
        setup.load_vocabularies_functionalproperties()
        endpoint_exploration.retrieve_functionalproperties_detectschemas()
        local_manipulation.voting_functionalproperties()
    start_time = time.time()
    # While there are same:Target in the current iteration named graph
    while len(resources_list) != 0:
        print(len(resources_list))
        print(resources_list)
        # :label: S1
        endpoint_exploration.retrieve_sameas(iteration)
        if Config.FUNC_PROP:
            # :label: (I)FP1 and (I)FP2
            endpoint_exploration.retrieve_functionalproperties_links1(iteration)
            endpoint_exploration.retrieve_functionalproperties_links2(iteration)
        # :label: R1 and R2 (CR1 is called by these functions)
        error_detection.rotten_sameas(iteration)
        error_detection.rotten_sameas2(iteration)
        iteration += 1
        # Polling, :label: T1
        resources_list = local_manipulation.get_targets(iteration)
    error_detection.rotten_sameas2(iteration)

    print("--- %s seconds ---" % (time.time() - start_time))
