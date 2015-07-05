from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres, ERROR, WARNING, DEBUG
import tdclient

class TreasureDataFdw (ForeignDataWrapper):
    """
    A foreign data wrapper for TreasureData
    """
    
    def __init__(self, options, columns):
        super(TreasureDataFdw, self).__init__(options, columns)
        if 'apikey' not in options:
            log_to_postgres('Parameter "apikey" is required.', ERROR)
        self.apikey = options.get("apikey", None)
        
        if 'database' not in options:
            log_to_postgres('Parameter "database" is required.', ERROR)
        self.database = options.get("database", None)
        
        if not any(x in options for x in ['table', 'query']):
            log_to_postgres('Parameter "table" or "query" parameter is required.', ERROR)
        self.table = options.get("table", None)
        self.query = options.get("query", None)

        self.query_engine = options.get("query_engine", 'presto')
            
        self.columns = columns

    def execute(self, quals, columns):
        if self.query:
            statement = self.query
        else:
            statement = "SELECT " + ",".join(self.columns.keys()) + " FROM " + self.table
        
        log_to_postgres('TreasureData query: ' + unicode(statement), DEBUG)

        try:
            with tdclient.Client(apikey = self.apikey) as td:
                job = td.query(self.database, statement, type=self.query_engine)
                job.wait()
                for row in job.result():
                    i = 0
                    record = {}
                    for column_name in self.columns:
                        record[column_name] = row[i]
                        i += 1
                    yield record
        except Exception, e:
            log_to_postgres(str(e), ERROR)

