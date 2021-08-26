from rdflib import Graph, ConjunctiveGraph
from SPARQLWrapper import SPARQLWrapper, JSON, N3, XML


class Helper(object):
    @staticmethod
    def insert_graph(endpoint: str, data: ConjunctiveGraph, named_graph: str, prefixes: str = ""):
        """
        Inserts data from a ConjunctiveGraph in a triplestore.
        :param endpoint: str, URL of the triplestore in which we insert the data.
        :param data: ConjunctiveGraph, graph containing the data.
        :param named_graph: str, named graph where to insert the data.
        :param prefixes: str, prefixes used in the SPARQL query.
        """
        gs = ConjunctiveGraph('SPARQLUpdateStore')
        gs.open((endpoint, endpoint))
        query = """
        %s
        INSERT DATA { 
            GRAPH %s {
               %s
            }
        }
        """ % (prefixes, named_graph, data.serialize(format='nt').decode("utf-8"))
        gs.update(query)

    @staticmethod
    def insert_array(endpoint: str, data: [str], named_graph: str, prefixes: str = ""):
        """
        Inserts data from a list in a triplestore.
        :param endpoint: str, URL of the triplestore in which we insert the data.
        :param data: list, data in RDF.
        :param named_graph: str, named graph where to insert the data.
        :param prefixes: str, prefixes used in the SPARQL query.
        """
        sparql = SPARQLWrapper(endpoint)
        sparql.method = 'POST'
        sparql.setRequestMethod('postdirectly')
        sparql.setQuery("""
            %s
            INSERT DATA {
              GRAPH %s {
                %s
              }
            }
        """ % (prefixes, named_graph, '\n'.join(data)))
        sparql.query()

    @staticmethod
    def non_ascii_characters_handling(function, handle_non_ascii: bool = False, iterator: int = 1,
                                      sparql_events: str = "", dataset_options: str = "",
                                      target_options: str = "") -> [str]:
        """
        Allows to handle non-ASCII characters when generating queries for remote endpoints.
        :param function: Function used to generate patterns of SPARQL queries (the function may return a String or
        tuple of Strings).
        :param handle_non_ascii: bool, Use or not use non-ASCII characters in requests
        (some endpoints do not support them, we have implemented methods to detect them).
        :param iterator: int, iteration of the algorithm.
        :param sparql_events: String, SPARQL Events of the Corese engine
        (https://ns.inria.fr/sparql-extension/event.html#event).
        :param dataset_options: String, Options on the available SPARQL endpoints.
        :param target_options: String, Options on the same:Target resources.
        :return: List of String, list of the different queries to execute on an endpoint.
        """
        results = []
        if handle_non_ascii:
            custom_dataset_options = dataset_options + "; same:supportsNonASCIICharacters false"
            custom_target_options = target_options + "FILTER(!REGEX(str(?URITarget), \"[^\\\\x00-\\\\x7F]\", \"i\"))"
            f_result = function(iterator=iterator, sparql_events=sparql_events,
                                dataset_options=custom_dataset_options, target_options=custom_target_options)
            if type(f_result) is tuple:
                results += list(f_result)
            else:
                results.append(f_result)

            custom_dataset_options = dataset_options + "; same:supportsNonASCIICharacters true"
            custom_target_options = target_options
            f_result = function(iterator=iterator, sparql_events=sparql_events,
                                dataset_options=custom_dataset_options, target_options=custom_target_options)
            if type(f_result) is tuple:
                results += list(f_result)
            else:
                results.append(f_result)
        else:
            custom_target_options = target_options + "FILTER(!REGEX(str(?URITarget), \"[^\\\\x00-\\\\x7F]\", \"i\"))"
            f_result = function(iterator=iterator, sparql_events=sparql_events, dataset_options=dataset_options,
                                target_options=custom_target_options)
            if type(f_result) is tuple:
                results += list(f_result)
            else:
                results.append(f_result)
        return results

    @staticmethod
    def removesuffix(string: str, suffix: str, /) -> str:
        """
        Removes a suffix of a String.
        :param string: String, String with a suffix to remove.
        :param suffix: String, suffix to remove.
        :return: String without suffix.
        """
        if string.endswith(suffix):
            return string[:-len(suffix)]
        else:
            return string[:]
