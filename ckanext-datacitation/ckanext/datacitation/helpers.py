from ckanext.datacitation.query_store import QueryStore
from ckan.common import config


class CurrentShowedPid(object):
    def __init__(self):
        self._pid = None

    @property
    def pid(self):
        return self._pid

    @pid.setter
    def pid(self, value):
        self._pid = value


current_showed_pid = CurrentShowedPid()


def datacitation_show_citation_info():
    if current_showed_pid.pid:
        qs = QueryStore()
        query = qs.retrieve_query(current_showed_pid.pid)
        site_url = config.get('ckan.site_url', None)

        if query:
            result = {
                'pid': query.id,
                'resource_id': query.resource_id,
                'resolve_url': site_url + '/querystore/view_query?id=' + str(query.id)
            }
            return result


def initiliaze_pid(new_pid):
    current_showed_pid.pid = new_pid


def refine_results(results, column_names):
    data = []
    for row in results:
        data.append(row_to_dict(row, column_names))
    return data


def row_to_dict(row, column_names):
    d = {}
    for column in column_names:
        d[column] = str(getattr(row, column))

    return d
