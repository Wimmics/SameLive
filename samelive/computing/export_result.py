import traceback
from samelive.utils.config import Config
from samelive.utils.iodata import Output
from SPARQLWrapper import SPARQLWrapper, JSON, N3, XML

master_endpoint = Config.master_endpoint

sparql = SPARQLWrapper(master_endpoint)
sparql.method = 'POST'
sparql.setReturnFormat(JSON)
sparql.setQuery("""
    PREFIX same: <https://ns.inria.fr/same/same.owl#>
    SELECT DISTINCT ?URITarget
    WHERE {
        ?URITarget a same:Target
    }
""")
resources = []
try:
    json = sparql.query().convert()["results"]["bindings"]
    resources = [j["URITarget"]["value"] for j in json]
except Exception as err:
    traceback.print_tb(err)

data = [["new_eq_id", "term"]]
# Value to distinguish the set
i = 1
for v in resources:
    data.append([i, v])

path = Config.project_path + "/resource/evaluation/O1.csv"
Output().save_csv(data, path)

sparql.setQuery("""
    PREFIX same: <https://ns.inria.fr/same/same.owl#>
    SELECT DISTINCT ?URITarget
    WHERE {
      {
        ?URITarget a same:Rotten
      } UNION {
        ?URITarget a same:Target
      }
    }
""")
resources = []
try:
    json = sparql.query().convert()["results"]["bindings"]
    resources = [j["URITarget"]["value"] for j in json]
except Exception as err:
    traceback.print_tb(err)

data = [["new_eq_id", "term"]]
# Value to distinguish the set
i = 1
for v in resources:
    data.append([i, v])

path = Config.project_path + "/resource/evaluation/O2.csv"
Output().save_csv(data, path)
