from ckanext.datastore.backend.postgres import DatastorePostgresqlBackend,identifier, get_write_engine,_get_fields_types,validate
import logging
from ckanext.datacitation.converter import hash_query_result,hash_query,convert_to_sql_query
from ckanext.datacitation.query_store import QueryStore
from sqlalchemy import func
from psycopg2.extras import DateTimeTZRange
from datetime import datetime
from ckanext.datacitation.helpers import initiliaze_pid,refine_results
import sys
import collections
from ckanext.datastore.backend import InvalidDataError
import ckan.plugins.toolkit as toolkit
reload(sys)
sys.setdefaultencoding('utf8')

log = logging.getLogger(__name__)

QUERY_STORE = QueryStore()


def create_op_type_trigger(table, table_history,connection):
    connection.execute(
        u'''CREATE OR REPLACE FUNCTION add_operation_type() RETURNS trigger AS $$
        BEGIN
            IF (TG_OP = 'DELETE') THEN
                UPDATE {table_history} SET op_type = 'DELETE' WHERE upper(sys_period) = (SELECT MAX(upper(sys_period)) FROM {table_history});
            ELSIF (TG_OP = 'UPDATE') THEN
                UPDATE {table_history} SET op_type = 'UPDATE' WHERE upper(sys_period) = (SELECT MAX(upper(sys_period)) FROM {table_history});
            END IF;
            RETURN NULL;
        END;
        $$ 
        LANGUAGE plpgsql;
        CREATE TRIGGER {trigger}
        AFTER INSERT OR UPDATE OR DELETE ON {table}
        FOR EACH ROW EXECUTE PROCEDURE add_operation_type();'''.format(table_history=table_history,
                                                                       trigger=identifier(table + '_trigger'),
                                                                       table=table)
    )


def create_versioning_trigger(data_dict,connection):
    connection.execute(
        u'''CREATE TRIGGER {trigger}
            BEFORE INSERT OR UPDATE OR DELETE ON {table}
            FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period',
                                          '{table_history}',
                                          true);'''.format(
            trigger=identifier(data_dict['resource_id'] + '_trigger'), table=identifier(data_dict['resource_id']),
            table_history=identifier(data_dict['resource_id'] + '_history')))



def create_history_table(data_dict,engine):
    columns = u", ".join([u'{0} {1}'.format(
        identifier(f['id']), f['type']) for f in data_dict['fields']])

    engine.execute(
        u' CREATE TABLE IF NOT EXISTS "{name}"({columns});'.format(
            name=data_dict['resource_id'],
            columns=columns
        )
    )

def postgres_querystore_resolve(query):
    engine = get_write_engine()
    connection = engine.connect()
    if query:
        if 'WHERE' in query.query:
            where = u'''WHERE (lower(sys_period) <='{exec_timestamp}') AND (('{exec_timestamp}' < upper(sys_period)) OR upper(sys_period) IS NULL) AND'''.format(
                exec_timestamp=query.exec_timestamp)
            history_query = (query.query.replace(query.resource_id, query.resource_id + '_history')).replace('WHERE',
                                                                                                             where)
            select = u'''{query}
                           UNION {history_query} ORDER BY _id'''.format(query=query.query.replace('WHERE', where),
                                                           history_query=history_query)
        else:
            where = u'''WHERE (lower(sys_period) <='{exec_timestamp}') AND (('{exec_timestamp}' < upper(sys_period)) OR upper(sys_period) IS NULL)'''.format(
                exec_timestamp=query.exec_timestamp)
            history_query = (query.query.replace(query.resource_id, query.resource_id + '_history')) + ' ' + where
            select = u'''{query} UNION {history_query} ORDER BY _id'''.format(query=query.query + ' ' + where,
                                                                           history_query=history_query)

        result = connection.execute(select)

        return result
    else:
        return None


def is_query_needed(query):
    if 'DISTINCT' in query:
        return False

    elif 'LIMIT 0 OFFSET 0' in query:
        return False
    else:
        return True


def detect_and_delete_toDeleted_rows(data_dict, connection,new_record,primary_key):
    '''This methode returns a tuple. Bool type is to determine if it is create mode
     or edit mode'''
    select = u'''SELECT * FROM "{table}"'''.format(table=data_dict['resource_id'])
    rs = connection.execute(select)
    old_record = refine_results(rs, rs.keys())

    old_ids=[]
    for dict in old_record:
        id=dict.get(primary_key,None)
        old_ids.append(int(id))

    new_ids=[]
    for dict in new_record:
        id=dict.get(primary_key,None)
        new_ids.append(id)

    rows_to_delete=list(set(old_ids) - set(new_ids))

    new_record_after_delete = [dict for dict in new_record if dict.get(primary_key,None) not in rows_to_delete]

    if len(rows_to_delete) != len(old_ids):
        for id in rows_to_delete:
            delete_sql=u'''DELETE FROM {table} WHERE {primary_key}={id}'''.format(table=identifier(data_dict['resource_id']),primary_key=identifier(primary_key),id=id)
            connection.execute(delete_sql)

        return True,new_record_after_delete
    else:
        return False,new_record


def detect_updated_rows(data_dict,connection,new_record,primary_key):
    select = u'''SELECT * FROM "{table}"'''.format(table=data_dict['resource_id'])
    rs = connection.execute(select)
    old_record = refine_results(rs, rs.keys())
    for dict in old_record:
        # exclude the fields that added after creation
        del dict['_id']
        del dict['_full_text']
        del dict['sys_period']

    new_record_unicoded=[]
    for dict in new_record:
        dict = {unicode(k): v.encode('utf-8') if isinstance(v,unicode)  else str(v) for k, v in dict.items()}
        new_record_unicoded.append(dict)



    updated_records=[]
    unchanged_records=[]
    for old_dict in old_record:
        for new_dict in new_record_unicoded:
            new_dict = collections.OrderedDict(sorted(new_dict.items()))
            old_dict=collections.OrderedDict(sorted(old_dict.items()))
            if int(new_dict.get(primary_key,None)) == int(old_dict.get(primary_key,None)):
                if new_dict.values() !=old_dict.values():
                    updated_records.append(new_dict)

                unchanged_records.append(int(new_dict.get(primary_key,None)))

    insert_data=[]
    for dict in new_record:
        if dict.get(primary_key,None) not in unchanged_records:
            insert_data.append(dict)


    return insert_data,updated_records



def find_primary_key(records):
    numeric_fiels=[]
    for dict in records:
        for key, value in dict.iteritems():
            try:
                if value is not None:
                    int(value)
                    numeric_fiels.append(key)
            except ValueError:
                pass

    primary_key_candidates={}
    for field in numeric_fiels:
        values=[]
        for dict in records:
            for key,value in dict.iteritems():
                if key== field:
                    values.append(value)

        primary_key_candidates[field]=values

    for key in primary_key_candidates:
        if len(primary_key_candidates.get(key, None)) <= len(set(primary_key_candidates.get(key, None))):
            return key

    return None




class VersionedDatastorePostgresqlBackend(DatastorePostgresqlBackend,object):

    def __init__(self):
        self.engine = get_write_engine()

    def create(self, context, data_dict):
        print 'CONTEXT'
        print context
        connection = self.engine.connect()
        primary_key = find_primary_key(data_dict.get('records'))
        if primary_key is None:
            #datacitation extension will not be activated because there
            #is no unique field given
            if super(VersionedDatastorePostgresqlBackend, self).resource_exists(data_dict['resource_id']):
                super(VersionedDatastorePostgresqlBackend, self).delete(context,data_dict)
            return super(VersionedDatastorePostgresqlBackend, self).create(context, data_dict)
        else:
            #datacitation extension will be activated, unique field is given
            if super(VersionedDatastorePostgresqlBackend, self).resource_exists(data_dict['resource_id']):
                if not data_dict.get('records'):
                    return
                records=data_dict.get('records',None)
                isEditMode,record_after_delete=detect_and_delete_toDeleted_rows(data_dict, connection, records,primary_key)

                if isEditMode:
                    #TODO check if the number of columns is equal

                    data_dict['method'] = 'update'
                    data_dict['primary_key'] = primary_key
                    insert_data,updated_rows=detect_updated_rows(data_dict, connection, record_after_delete,primary_key)
                    data_dict['records'] = updated_rows
                    super(VersionedDatastorePostgresqlBackend, self).upsert(context, data_dict)
                    data_dict['method']='insert'
                    data_dict['records']=insert_data
                    return super(VersionedDatastorePostgresqlBackend, self).upsert(context,data_dict)
                else:
                    return super(VersionedDatastorePostgresqlBackend, self).create(context,data_dict)
            else:
                fields = data_dict.get('fields', None)
                records = data_dict.get('records', None)
                fields.append(
                    {
                        "id": "sys_period",
                        "type": "tstzrange"
                    }
                )
                if records is not None:
                    for r in records:
                        r['sys_period'] = DateTimeTZRange(datetime.now(), None)

                data_dict['primary_key'] = primary_key
                data_dict['fields'] = fields
                data_dict['records'] = records
                datastore_fields = [
                    {'id': '_id', 'type': 'integer'},
                    {'id': '_full_text', 'type': 'tsvector'},
                ]
                extra_field = [
                    {
                        "id": "op_type",
                        "type": "text"
                    }
                ]
                fields_of_history_table = datastore_fields + list(fields) + extra_field
                history_data_dict = {
                    "fields": fields_of_history_table,
                    "resource_id": data_dict['resource_id'] + '_history'
                }
                create_history_table(history_data_dict, self.engine)
                result = super(VersionedDatastorePostgresqlBackend, self).create(context, data_dict)
                create_versioning_trigger(data_dict, connection)
                create_op_type_trigger(identifier(data_dict['resource_id']),
                                       identifier(data_dict['resource_id'] + '_history'), connection)
                connection.close()
                return result

    def delete(self, context, data_dict):
        return NotImplementedError()


    def search(self, context, data_dict):
        data_dict_copy=data_dict
        connection=self.engine.connect()
        context['connection']=connection
        validate(context, data_dict_copy)
        fields_types = _get_fields_types(
            context['connection'], data_dict_copy['resource_id'])
        query_dict = {
            'select': [],
            'sort': [],
            'where': []
        }

        query_dict=super(VersionedDatastorePostgresqlBackend, self).datastore_search(context,data_dict_copy,fields_types,query_dict)

        query = convert_to_sql_query(query_dict, data_dict_copy)


        if is_query_needed(query):
            result = connection.execute(query)
            pid=QUERY_STORE.store_query(func.now(), query, hash_query(query), hash_query_result(result), data_dict_copy['resource_id'])
            initiliaze_pid(pid)

        connection.close()

        return super(VersionedDatastorePostgresqlBackend, self).search(context,data_dict)
