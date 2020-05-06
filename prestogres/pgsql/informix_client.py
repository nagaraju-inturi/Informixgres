import os
import time
import IfxPy
import IfxPyDbi as dbapi2
from builtins import str

try: import simplejson as json
except ImportError: import json

VERSION = "0.1.0"

class ClientSession(object):
    def __init__(self, server, user, source=None, catalog=None, schema=None, debug=False):
        self.server = server
        self.user = user
        self.source = source
        self.catalog = catalog
        self.schema = schema
        self.debug = debug

class StatementStats(object):
    def __init__(self, state=None, scheduled=None, nodes=None, total_splits=None, queued_splits=None, running_splits=None, completed_splits=None, user_time_millis=None, cpu_time_millis=None, wall_time_millis=None, processed_rows=None, processed_bytes=None):
        self.state = state
        self.scheduled = scheduled
        self.nodes = nodes
        self.total_splits = total_splits
        self.queued_splits = queued_splits
        self.running_splits = running_splits
        self.completed_splits = completed_splits
        self.user_time_millis = user_time_millis
        self.cpu_time_millis = cpu_time_millis
        self.wall_time_millis = wall_time_millis
        self.processed_rows = processed_rows
        self.processed_bytes = processed_bytes
        #self.root_stage = root_stage

    @classmethod
    def decode_dict(cls, dic):
        return StatementStats(
                state=dic.get("state"),
                scheduled=dic.get("scheduled"),
                nodes=dic.get("nodes"),
                total_splits=dic.get("totalSplits"),
                queued_splits=dic.get("queuedSplits"),
                running_splits=dic.get("runningSplits"),
                completed_splits=dic.get("completedSplits"),
                user_time_millis=dic.get("userTimeMillis"),
                cpu_time_millis=dic.get("cpuTimeMillis"),
                wall_time_millis=dic.get("wallTimeMillis"),
                processed_rows=dic.get("processedRows"),
                processed_bytes=dic.get("processedBytes"),
                #root_stage=StageStats.decode_dict(dic["rootStage")),
                )

class Column(object):
    def __init__(self, name, type, len):
        self.name = name
        self.type = type
        self.len = len

    @classmethod
    def decode_dict(cls, dic):
        return Column(
                name=dic.get("name"),
                type=dic.get("type"),
                len=dic.get("len"),
                )

class ErrorLocation(object):
    def __init__(self, line_number, column_number):
        self.line_number = line_number
        self.column_number = column_number

    @classmethod
    def decode_dict(cls, dic):
        return ErrorLocation(
                line_number=dic.get("lineNumber"),
                column_number=dic.get("columnNumber"),
                )

class FailureInfo(object):
    def __init__(self, type=None, message=None, cause=None, suppressed=None, stack=None, error_location=None):
        self.type = type
        self.message = message
        self.cause = cause
        self.suppressed = suppressed
        self.stack = stack
        self.error_location = error_location

    @classmethod
    def decode_dict(cls, dic):
        return FailureInfo(
                type=dic.get("type"),
                message=dic.get("message"),
                cause=dic.get("cause"),
                suppressed=map(FailureInfo.decode_dict, dic["suppressed"]) if "suppressed" in dic else None,
                stack=dic.get("stack"),
                error_location=ErrorLocation.decode_dict(dic["errorLocation"]) if "errorLocation" in dic else None,
                )

class QueryError(object):
    def __init__(self, message=None, sql_state=None, error_code=None, error_location=None, failure_info=None):
        self.message = message
        self.sql_state = sql_state
        self.error_code = error_code
        self.error_location = error_location
        self.failure_info = failure_info

    @classmethod
    def decode_dict(cls, dic):
        return QueryError(
                message=dic.get("message"),
                sql_state=dic.get("sqlState"),
                error_code=dic.get("errorCode"),
                error_location=ErrorLocation.decode_dict(dic["errorLocation"]) if "errorLocation" in dic else None,
                failure_info=FailureInfo.decode_dict(dic["failureInfo"]) if "failureInfo" in dic else None,
                )

class QueryResults(object):
    def __init__(self, columns=None, data=None, stats=None, error=None):
        self.columns = columns
        self.data = data
        self.stats = stats
        self.error = error

    @classmethod
    def decode_dict(cls, columns, data):
        return QueryResults(
                columns=columns,
                data=data,
                stats=None,
                error=None
                )

class PrestoException(Exception):
    pass

class PrestoHttpException(PrestoException):
    def __init__(self, status, message):
        PrestoException.__init__(self, message)
        self.status = status

class PrestoClientException(PrestoException):
    pass

class PrestoQueryException(PrestoException):
    def __init__(self, message, query_id, error_code, failure_info):
        PrestoException.__init__(self, message)
        self.query_id = query_id
        self.error_code = error_code
        self.failure_info = failure_info

class StatementClient(object):
    def __init__(self, conn, query, **options):
        self.conn = conn
        self.query = query
        self.options = options

        self.closed = False
        self.exception = None
        self.results = None
        self._post_query_request()

    def _post_query_request(self):

        self.cur = self.conn.cursor()
        rowcnt = self.cur.execute (str(self.query))
        if self.cur.description:
            rows = self.cur.fetchall()
            cols = []
            num_columns = IfxPy.num_fields(self.cur.stmt_handler)
            for column_index in range(num_columns):
                name = IfxPy.field_name(self.cur.stmt_handler, column_index)
                type = IfxPy.field_type(self.cur.stmt_handler, column_index)
                len = IfxPy.field_precision(self.cur.stmt_handler, column_index)
                type = type.upper()
                if (type == "STRING"):
                   type = "character(" + str(len) + ")"
                cols.append(Column(name, type, len))

            for c in cols:
                print c.name 
                print c.type 
            print "Done for now..."
            self.results = QueryResults.decode_dict(columns=cols, data=rows)
        else:
            cols = []
            rows = []
            cols.append(Column("count", "INT", 11))
            tupl = ()
            tupl = tupl + (int(rowcnt),)
            rows.append(tupl)
            for c in cols:
                print c.name 
                print c.type 
            print rows
            print "Done for now..."
            self.results = QueryResults.decode_dict(columns=cols, data=rows)
            self.conn.commit()
            #self.close()
   

    @property
    def is_query_failed(self):
        return self.results.error is not None

    @property
    def is_query_succeeded(self):
        return self.results.error is None and self.exception is None and not self.closed

    @property
    def has_next(self):
        return None
        #return self.results.next_uri is not None

    def advance(self):
        if self.closed or not self.has_next:
            return False
        return False

        start = time.time()
        attempts = 0
        row = None

        while True:
            try:
                row = self.cur.fetchrow()
                #row = None
            except Exception as e:
                self.exception = e
                print ('ERROR5: Nagaraju')
                raise

            if row:
                self.results = QueryResults.decode_dict(None, row)
                return True

            if (time.time() - start) > 2*60*60 or self.closed:
                break

        self.exception = PrestoHttpException(408, "Error fetching next")  # TODO error class
        raise self.exception

    def cancel_leaf_stage(self):
        return False

    def close(self):
        if self.closed:
            return

        self.cancel_leaf_stage()

        self.closed = True

class Query(object):
    @classmethod
    def start(cls, query, **options):
        ConStr = "SERVER=informix;DATABASE=test;HOST=127.0.0.1;SERVICE=60000;UID=informix;PWD=changeme;"
        try:
            print options["server"]
            conn = dbapi2.connect( str(options["server"]), str(''), str(''), str(''), str(''))
        except Exception as e:
            print ('ERROR: Connect failed')
            print ( e )
            quit()

        return Query(StatementClient(conn, query, **options))

    def __init__(self, client):
        self.client = client

    def _wait_for_columns(self):
        while self.client.results.columns is None and self.client.advance():
            pass

    def _wait_for_data(self):
        while self.client.results.data is None and self.client.advance():
            pass

    def columns(self):
        print ("Nagaraju: called columns()")
        #self._wait_for_columns()

        #if not self.client.is_query_succeeded:
        #    self._raise_error()

        if self.client.results.columns is None:
           print ("Nagaraju: columns are None")
        else:
           print self.client.results.columns
        return self.client.results.columns

    def results(self):
        self._wait_for_data()

        client = self.client

        if not client.is_query_succeeded:
            self._raise_error()

        print "Nagaraju : calling columns from results()"
        if self.columns() is None:
            raise PrestoException("Query %s has no columns" % client.results.id)

        while True:
            if client.results.data is None:
                break

            for row in client.results.data:
                print ("Nagaraju: yield row")
                yield list(row)

            if not client.advance():
                print ("Nagaraju: results() advance no data")
                break

            if client.results.data is None:
                print ("Nagaraju: results() no data")
                break

    def cancel(self):
        self.client.cancel_leaf_stage()

    def close(self):
        self.client.cancel_leaf_stage()

    def _raise_error(self):
        if self.client.closed:
            raise PrestoClientException("Query aborted by user")
        elif self.client.exception is not None:
            print ('ERROR2: Nagaraju')
            raise self.client.exception
        elif self.client.is_query_failed:
            results = self.client.results
            error = results.error
            print ('ERROR3: Nagaraju')
            if error is None:
                raise PrestoQueryException("Query %s failed: (unknown reason)" % results.id, None, None)
            raise PrestoQueryException("Query %s failed: %s" % (results.id, error.message), results.id, error.error_code, error.failure_info)

class Client(object):
    def __init__(self, **options):
        self.options = options

    def query(self, query):
        return Query.start(query, **self.options)

    def run(self, query):
        q = Query.start(query, **self.options)
        try:
            columns = q.columns()
            if columns is None:
                return [], []
            rows = []
            map(rows.append, q.results())
            return columns, rows
        finally:
            q.close()

