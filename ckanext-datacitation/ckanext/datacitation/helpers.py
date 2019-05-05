from ckanext.datacitation.query_store import QueryStore
from ckan.common import config
from ckan.lib.base import h

pid=None
def show_citation_info():
    print '===PID IN HELPERS=='
    print pid
    if pid:
        qs = QueryStore()
        query = qs.retrieve_query(pid)
        r = h.get_request_param('filters')
        print r
        site_url = config.get('ckan.site_url', None)

        result = {
            'pid': query.id,
            'resource_id': query.resource_id,
            'resolve_url': site_url + '/querystore/view_query?id=' + str(query.id)
        }
        return result


def initiliaze_pid(new_pid):
    global pid
    pid=new_pid