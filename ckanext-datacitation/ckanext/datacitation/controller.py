from ckanext.datastore.writer import csv_writer, json_writer, xml_writer
import logging
from ckan.lib.base import BaseController,h,render,response,abort
from ckan.logic import get_action
log=logging.getLogger(__name__)

import ckan.logic as logic
import ckan.lib.helpers as h

import ckan.lib.plugins

import ckan.lib.render


NotFound = logic.NotFound
NotAuthorized = logic.NotAuthorized
ValidationError = logic.ValidationError
check_access = logic.check_access
tuplize_dict = logic.tuplize_dict
clean_dict = logic.clean_dict
parse_params = logic.parse_params
flatten_to_string_key = logic.flatten_to_string_key

lookup_package_plugin = ckan.lib.plugins.lookup_package_plugin


def history_dump_to(pid, output, fmt, options):
    if fmt == 'csv':
        writer_factory = csv_writer
    elif fmt == 'json':
        writer_factory = json_writer
    elif fmt == 'xml':
        writer_factory = xml_writer
    else:
        abort(501, 'Only dump to csv, json or xml file supported!')

    def start_writer(fields):
        bom = options.get(u'bom', False)
        return writer_factory(output, fields, "{0}_dump".format(pid), bom)

    def result_page():
        return get_action('querystore_resolve')(None, dict({'pid': pid, 'records_format': fmt}))

    result = result_page()



    log.debug('start writing dump...')

    with start_writer(result['fields']) as wr:
            records = result['result_set']

            log.debug("writing: {0}".format(records))

            wr.write_records(records)

            log.debug("writing done: {0}".format(records))


class QueryStoreController(BaseController):


    def view_history_query(self):
        id = h.get_param_int('id')
        result = get_action('querystore_resolve')(None, {'pid': id})

        if result:
            return render('versioneddatastore/query_view.html', extra_vars={'query': result['query'],
                                                                    'result_set': result['result_set'],
                                                                    'count': len(result['result_set']),
                                                                    'projection': result['column_names'],
                                                                    'resource_id':result['resource_id']})
        else:
            abort(404,'The given PID does not exist')



    def dump_history_result_set(self):
        pid = int(h.get_request_param('id'))
        format = h.get_request_param('format')

        if h.get_request_param('bom') and h.get_request_param('bom') in ['True', 'true']:
            bom = True
        else:
            bom = False


        parameters = [
            pid,
            response,
            format,
            {u'bom': bom}]

        log.debug('history_dump_to parameters: {0}'.format(parameters))

        history_dump_to(
            pid,
            response,
            fmt=format,
            options={u'bom': bom}
        )
