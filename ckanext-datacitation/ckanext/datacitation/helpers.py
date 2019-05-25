from ckanext.datacitation.query_store import QueryStore
from ckan.common import config

pid=None
def datacitation_show_citation_info():
    if pid:
        qs = QueryStore()
        query = qs.retrieve_query(pid)
        site_url = config.get('ckan.site_url', None)

        if query:
            result = {
                'pid': query.id,
                'resource_id': query.resource_id,
                'resolve_url': site_url + '/querystore/view_query?id=' + str(query.id)
            }
            return result


def initiliaze_pid(new_pid):
    global pid
    pid=new_pid



def refine_results(results,column_names):
    data = []
    for row in results:
        data.append(row_to_dict(row, column_names))
    return data

def row_to_dict(row, column_names):
    d = {}
    for column in column_names:
        d[column] = str(getattr(row, column))

    return d