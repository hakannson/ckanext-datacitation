import logging
from ckanext.datacitation.postgresdb_controller import PostgresDbController

log=logging.getLogger(__name__)

def querystore_resolve(context, data_dict):
    controller = PostgresDbController()

    pid = data_dict.get('pid')
    #skip = data_dict.get('offset', None)
    #limit = data_dict.get('limit', None)

   # if skip:
   #     skip = int(skip)
   # if limit:
   #     limit = int(limit)

    records_format = data_dict.get('records_format', 'objects')

   # log.debug('querystore_resolve parameters {0}'.format([pid, skip, limit, records_format]))

    result = controller.retrieve_stored_query(pid)

    #log.debug('querystore_resolve result: {0}'.format(result))

    return result



def datastore_create(context,data_dict):
    print '=========BABBBBO============='