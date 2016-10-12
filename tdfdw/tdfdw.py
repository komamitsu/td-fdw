import string
from multicorn import ForeignDataWrapper
from multicorn import ANY, ALL
from multicorn.utils import log_to_postgres, ERROR, WARNING, DEBUG
import tdclient

class TreasureDataFdw (ForeignDataWrapper):
    """
    A foreign data wrapper for TreasureData
    """
    
    def __init__(self, options, columns):
        super(TreasureDataFdw, self).__init__(options, columns)
        self.endpoint = options.get("endpoint", None)

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

    def encode_value(self, value):
        try:
            string_alike = basestring
        except NameError:
            string_alike = str

        if isinstance(value, string_alike):
            return "'%s'" % (string.replace(value, "'", "''"))
        else:
            return value

    def create_cond(self, quals):
        cond = ''
        first_qual = True
        for qual in quals:
            if first_qual:
                first_qual = False
            else:
                cond += ' AND '

            cond += '('
            if qual.value == None:
                if qual.operator == '=':
                    cond += '%s IS NULL' % (qual.field_name)
                elif qual.operator == '<>':
                    cond += '%s IS NOT NULL' % (qual.field_name)
                else:
                    log_to_postgres('Unexpected qual: %s' % (qual), ERROR)
            else:
                operator = qual.operator[0] if qual.is_list_operator else qual.operator
                if operator == '~~':
                    operator = 'LIKE'
                elif operator == '!~~':
                    operator = 'NOT LIKE'

                values = qual.value if qual.list_any_or_all else [qual.value]
                first_value = True
                for value in values:
                    if first_value:
                        first_value = False
                    else:
                        if qual.list_any_or_all == ANY:
                            cond += ' OR '
                        elif qual.list_any_or_all == ALL:
                            cond += ' AND '
                        else:
                            log_to_postgres('Unexpected qual: %s' % (qual), ERROR)

                    cond += '%s %s %s' % (qual.field_name, operator, self.encode_value(value))
            cond += ')'
        return cond

    def execute(self, quals, columns):
        if self.query:
            statement = self.query
        else:
            cond = self.create_cond(quals)
            statement = "SELECT %s FROM %s" % (",".join(self.columns.keys()), self.table)
            if cond != '':
                statement += ' WHERE %s' % (cond)
        
        log_to_postgres('TreasureData query: ' + str(statement), DEBUG)

        try:
            with tdclient.Client(apikey = self.apikey, endpoint = self.endpoint) as td:
                job = td.query(self.database, statement, type=self.query_engine)
                job.wait()
                for row in job.result():
                    i = 0
                    record = {}
                    for column_name in self.columns:
                        record[column_name] = row[i]
                        i += 1
                    yield record
        except Exception as e:
            log_to_postgres(str(e), ERROR)

