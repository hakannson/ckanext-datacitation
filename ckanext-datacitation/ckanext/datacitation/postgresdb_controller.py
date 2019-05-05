from ckanext.datacitation.query_store import QueryStore
from ckanext.datastore.backend.postgres import get_write_engine,_get_fields_types,_get_field_info,_result_fields
from ckanext.datastore.helpers import get_list
from StringIO import StringIO
import collections
import csv
import sys
reload(sys)
sys.setdefaultencoding('utf8')

def refine_results(results,column_names):
    """ Refine some search results. """
    # something here, maybe a request for search results

    data = []
    for row in results:
        data.append(row_to_dict(row, column_names))
    return data

def row_to_dict(row, column_names):
    d = {}
    for column in column_names:
        d[column] = str(getattr(row, column))

    return d


def convert_to_csv(result_set):
    output = StringIO()
    writer = csv.writer(output, delimiter=';', quotechar='"')

    for record in result_set:
        record = collections.OrderedDict(sorted(record.items()))
        writer.writerow(record.values())

    returnval = output.getvalue()
    output.close()
    return returnval

def exclude_sys_period(fields):
    new_fields=[]
    for dict in fields:
        if u'sys_period' not in dict.values():
            new_fields.append(dict)

    return new_fields

class PostgresDbController:

    def retrieve_stored_query(self,pid,records_format='objects'):
        qs=QueryStore()
        query=qs.retrieve_query(pid)
        engine = get_write_engine()
        connection = engine.connect()
        if query:
            if 'WHERE' in query.query:
                where = u'''WHERE (lower(sys_period) <='{exec_timestamp}') AND (('{exec_timestamp}' < upper(sys_period)) OR upper(sys_period) IS NULL) AND'''.format(
                    exec_timestamp=query.exec_timestamp)
                history_query=(query.query.replace(query.resource_id,query.resource_id+'_history')).replace('WHERE',where)
                select= u'''{query}
                        UNION {history_query}'''.format(query=query.query.replace('WHERE', where ),history_query=history_query)
            else:
                where = u'''WHERE (lower(sys_period) <='{exec_timestamp}') AND (('{exec_timestamp}' < upper(sys_period)) OR upper(sys_period) IS NULL)'''.format(
                    exec_timestamp=query.exec_timestamp)
                history_query = (query.query.replace(query.resource_id, query.resource_id + '_history')) +' '+ where
                select = u'''{query}
                                        UNION {history_query}'''.format(query=query.query+' '+where,
                                                                        history_query=history_query)
                print select


            rs= connection.execute(select)

            #column names as a list
            column_names = rs.keys()
            #to delete standard sys_period column for versioning
            #(is not necessary for the user, it has a technical meaning)
            del column_names[-1]
            search_result=refine_results(rs,column_names)

            result_dictionary = {
                'column_names': sorted(column_names),
                'result_set': search_result,
                'query': query
            }
            context = {'connection': connection}

            fields_types = _get_fields_types(
                context['connection'], query.resource_id)

            result_dictionary['fields'] = sorted(_result_fields(
                fields_types,
                _get_field_info(context['connection'], query.resource_id),
                get_list(result_dictionary.get('fields'))))

            # do not show sys_period column because, it has only
            # a technical meaning
            result_dictionary['fields']=exclude_sys_period(result_dictionary['fields'])

            if records_format == 'objects':
                result_dictionary['result_set'] = list(result_dictionary['result_set'])
            elif records_format == 'csv':
                result_dictionary['result_set'] = convert_to_csv(result_dictionary['result_set'])


            connection.close()

            return result_dictionary
        else:
            return None

