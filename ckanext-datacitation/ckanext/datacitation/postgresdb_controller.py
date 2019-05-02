from ckanext.datacitation.query_store import QueryStore
from ckanext.datastore.backend.postgres import get_write_engine
import sys
reload(sys)
sys.setdefaultencoding('utf8')

def refine_results(results,column_names):
    """ Refine some search results. """
    # something here, maybe a request for search results

    data = []
    for row in results:
        data.append(rowTodict(row,column_names))
    return data

def rowTodict(row,column_names):
    d = {}
    for column in column_names:
        d[column] = str(getattr(row, column))

    return d



class PostgresDbController:

    def retrieve_stored_query(self,pid):
        qs=QueryStore()
        query=qs.retrieve_query(pid)
        where=u'''WHERE (lower(sys_period) <='{exec_timestamp}') AND (('{exec_timestamp}' < upper(sys_period)) OR upper(sys_period) IS NULL) AND'''.format(exec_timestamp=query.exec_timestamp)
        history_query=(query.query.replace(query.resource_id,query.resource_id+'_history')).replace('WHERE',where)
        select= u'''{query}
                UNION {history_query} '''.format(query=query.query.replace('WHERE', where ),history_query=history_query)


        engine = get_write_engine()
        connection = engine.connect()
        rs= connection.execute(select)

        #column names as a list
        column_names = rs.keys()
        del column_names[-1]
        search_result=refine_results(rs,column_names)

        result_dictionary={
            'column_names':column_names,
            'result_set':search_result,
            'query':query
        }
        return result_dictionary
