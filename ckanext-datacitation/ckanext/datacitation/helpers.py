from ckanext.datacitation.query_store import QueryStore
from ckan.common import config

pid=None
def show_citation_info():
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