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
