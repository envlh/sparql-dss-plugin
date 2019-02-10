from dataiku.connector import Connector
from SPARQLWrapper import SPARQLWrapper2

class SparqlConnector(Connector):

    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.sparql_endpoint_url = self.config.get("sparql_endpoint_url", "")
        self.sparql_query = self.config.get("sparql_query", "")

    def get_read_schema(self):
        return None

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None, partition_id=None, records_limit = -1):
        sparql = SPARQLWrapper2(self.sparql_endpoint_url)
        sparql.setQuery(self.sparql_query)
        results = sparql.query()
        for e in results.bindings:
            a = {}
            for k in e:
                a[k] = e[k].value
            yield a
