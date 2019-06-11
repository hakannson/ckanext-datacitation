from ckanext.datastore.backend.postgres import DatastorePostgresqlBackend, identifier, get_write_engine, \
    _get_fields_types, validate, _get_engine_from_url
from ckan.common import config
from ckanext.datastore.backend import InvalidDataError
import ckan.plugins.toolkit as toolkit
import logging
from ckanext.datacitation.converter import hash_query_result, hash_query, convert_to_sql_query
from ckanext.datacitation.query_store import QueryStore
from sqlalchemy import func
import sqlalchemy
from psycopg2.extras import DateTimeTZRange
from datetime import datetime
from ckanext.datacitation.helpers import initiliaze_pid, refine_results
import sys
import collections
import time

reload(sys)
sys.setdefaultencoding('utf8')

log = logging.getLogger(__name__)

QUERY_STORE = QueryStore()

total = 0


class CurrentEntryProperties(object):
    def __init__(self):
        self._min_id = None
        self._max_id = None
        self._primary_key = None

    @property
    def min_id(self):
        return self._min_id

    @min_id.setter
    def min_id(self, value):
        self._min_id = value

    @property
    def max_id(self):
        return self._max_id

    @max_id.setter
    def max_id(self, value):
        self._max_id = value

    @property
    def primary_key(self):
        return self._primary_key

    @primary_key.setter
    def primary_key(self, value):
        self._primary_key = value


current_entry_properties = CurrentEntryProperties()


def create_op_type_trigger(table, table_history, connection):
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


def create_versioning_trigger(data_dict, connection):
    connection.execute(
        u'''CREATE TRIGGER {trigger}
            BEFORE INSERT OR UPDATE OR DELETE ON {table}
            FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period',
                                          '{table_history}',
                                          true);'''.format(
            trigger=identifier(data_dict['resource_id'] + '_trigger'), table=identifier(data_dict['resource_id']),
            table_history=identifier(data_dict['resource_id'] + '_history')))


def create_history_table(data_dict, engine):
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


def detect_deleted_rows(data_dict, connection, new_record, primary_key):
    '''This methode detects the deleted rows. It detects only if the dataset is getting edited
    otherwise there is nothing to do. It must be better if this informartion (Edit or Create)
    came from Listeners (update, save Listeners) directly. This would reduce the complexity
    in the backend'''
    detect_dict = {}
    new_ids = [dict.get(primary_key, None) for dict in new_record]
    new_ids.sort()

    resource_id = data_dict['resource_id']

    select = u'''SELECT MAX({primary_key}) FROM {table}'''. \
        format(table=identifier(resource_id), primary_key=identifier(primary_key))
    rs = connection.execute(select)
    max_id_in_database = rs.first()[0]

    print 'MAX_ID_IN_DATABASE'
    print max_id_in_database

    if new_ids[0] > max_id_in_database:
        # create mode therefore nothing do detect
        reset_min_max_id()
        detect_dict['mode'] = 'create'
        return detect_dict
    else:
        # edit mode detect deleted rows
        detect_dict['mode'] = 'edit'

        find_min_and_max_id(new_ids, primary_key, resource_id, connection)

        if current_entry_properties.min_id > current_entry_properties.max_id:
            current_entry_properties.min_id = current_entry_properties.max_id - 250

        print 'MIN_ID'
        print current_entry_properties.min_id

        print 'MAX_ID'
        print current_entry_properties.max_id

        select = u'''SELECT * FROM {table} WHERE {primary_key} BETWEEN {min} AND {max} ORDER BY {primary_key}'''. \
            format(table=identifier(data_dict['resource_id']), primary_key=identifier(primary_key),
                   min=current_entry_properties.min_id, max=current_entry_properties.max_id)

        print 'SELECT'
        print select
        rs = connection.execute(select)
        old_record = refine_results(rs, rs.keys())

        old_ids = [int(dict.get(primary_key, None)) for dict in old_record]

        print 'OLD_IDS'
        print old_ids
        # this to set the offset for the next chunk of data
        current_entry_properties.min_id = current_entry_properties.max_id + 1

        detect_dict['old_ids'] = old_ids
        detect_dict['old_record'] = old_record

        rows_to_delete = list(set(old_ids) - set(new_ids))

        print 'ROWS_TO_DELETE'
        print rows_to_delete
        if not rows_to_delete:
            detect_dict['new_record'] = new_record
            return detect_dict

        delete_deleted_rows(rows_to_delete, connection, data_dict, primary_key)

        new_record_after_delete = [dict for dict in new_record if dict.get(primary_key, None) not in rows_to_delete]

        detect_dict['new_record'] = new_record_after_delete

        return detect_dict


def reset_min_max_id():
    current_entry_properties.min_id = None
    current_entry_properties.max_id = None


def find_min_and_max_id(new_ids, primary_key, resource_id, connection):
    '''This is a manually pagination to detect the changes.
    CKAN push datas in chunks with the size 250. If something
    gets changed the offset (min_id) gets shifted. Therefore we
    need a mechanism to find the start id for the comparison with
    the new data'''

    if current_entry_properties.min_id is None or new_ids[0] < current_entry_properties.min_id:
        select = u'''SELECT MIN({primary_key}) FROM {table}'''. \
            format(table=identifier(resource_id), primary_key=identifier(primary_key))
        rs = connection.execute(select)
        current_entry_properties.min_id = rs.first()[0]

    current_entry_properties.max_id = current_entry_properties.min_id + 249
    last_id = new_ids[-1]

    if last_id > current_entry_properties.max_id:
        current_entry_properties.max_id = last_id


def delete_deleted_rows(rows_to_delete, connection, data_dict, primary_key):
    for id in rows_to_delete:
        delete_sql = u'''DELETE FROM {table} WHERE {primary_key}={id}'''.format(
            table=identifier(data_dict['resource_id']), primary_key=identifier(primary_key), id=id)
        connection.execute(delete_sql)


def detect_updated_rows(new_record, old_record, primary_key):
    for dict in old_record:
        # for the comparison we don't need all fields
        # exclude the fields that added during the creation of the database table
        del dict['_id']
        del dict['_full_text']
        del dict['sys_period']

    new_record_unicoded = []
    for dict in new_record:
        dict = {unicode(k): v.encode('utf-8') if isinstance(v, unicode) else str(v) for k, v in dict.items()}
        new_record_unicoded.append(dict)

    updated_records = []
    for old_dict in old_record:
        for new_dict in new_record_unicoded:
            new_dict = collections.OrderedDict(sorted(new_dict.items()))
            old_dict = collections.OrderedDict(sorted(old_dict.items()))
            if int(new_dict.get(primary_key, None)) == int(old_dict.get(primary_key, None)):
                if new_dict.values() != old_dict.values():
                    updated_records.append(new_dict)
                break

    return updated_records


def detect_inserted_rows(new_record, old_ids, primary_key):
    new_ids = [dict.get(primary_key, None) for dict in new_record]
    new_ids.sort()
    return [dict for dict in new_record if dict.get(primary_key, None) not in old_ids]


def find_primary_key(records):
    print '===CARE GELMEZ AGLAMAKTAN==='
    numeric_fiels = []
    for dict in records:
        for key, value in dict.iteritems():
            try:
                if value is not None:
                    int(value)
                    numeric_fiels.append(key)
            except ValueError:
                pass

    primary_key_candidates = {}
    for field in numeric_fiels:
        values = []
        for dict in records:
            for key, value in dict.iteritems():
                if key == field:
                    values.append(value)

        primary_key_candidates[field] = values

    for key in primary_key_candidates:
        if len(primary_key_candidates.get(key, None)) <= len(set(primary_key_candidates.get(key, None))):
            return key

    return None


def resource_exists(id):
    resources_sql = sqlalchemy.text(
        u'''SELECT 1 FROM "_table_metadata"
        WHERE name = :id AND alias_of IS NULL''')
    read_url = config['ckan.datastore.read_url']
    read_engine = _get_engine_from_url(read_url)
    results = read_engine.execute(resources_sql, id=id)
    res_exists = results.rowcount > 0
    return res_exists


def get_old_columns_number(connection, resource_id):
    '''
    to make generic the database name must come from
    ckan config file
    e.g.
    database_url=config.get('ckan.datastore.write_url',None)
    dabase_name=database_url.split('@')[1].split('/')'''

    query = u'''SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE table_catalog = 'datastore_default'
    AND table_name = {table_name}'''.format(table_name=identifier(resource_id))

    return connection.execute(query)


class VersionedDatastorePostgresqlBackend(DatastorePostgresqlBackend, object):

    def __init__(self):
        self.engine = get_write_engine()
        self.connection = get_write_engine().connect()

    def create(self, context, data_dict):
        u'''datacitation extension will only be activated if the dataset has
        an unique field otherwise it will proceed according to CKAN standard
        '''
        t0 = time.time()
        global total
        records = data_dict.get('records', None)

        current_entry_properties.primary_key = find_primary_key(records)

        if current_entry_properties.primary_key is None:
            raise InvalidDataError(
                toolkit._("The data has no unique field!"))
        else:
            if super(VersionedDatastorePostgresqlBackend, self).resource_exists(data_dict['resource_id']):
                # CKAN Datapusher pushes the entries in chunks of 250 entries
                # Because of that after pushing 250 entries, the table will exist.
                # Therefore if the table exists it does not automatically
                # indicate that it is an update. There is another manual check
                # to distinguish between UPDATE and CREATE.
                # If would be better, if it is determined by EventListeners

                if not records:
                    t1 = time.time()
                    delta = t1 - t0
                    print 'DELTA'
                    print delta
                    total = total + delta
                    print '==TOTAL=='
                    print total
                    return

                detect_dict = detect_deleted_rows(data_dict, self.connection, records,
                                                  current_entry_properties.primary_key)

                if detect_dict.get('mode', None) == 'create':
                    print '===BALLER_LOS==='
                    t1 = time.time()
                    delta = t1 - t0
                    print 'DELTA'
                    print delta
                    total = total + delta
                    print '==TOTAL=='
                    print total
                    return super(VersionedDatastorePostgresqlBackend, self).create(context, data_dict)

                old_record = detect_dict['old_record']
                record_after_delete = detect_dict['new_record']
                old_ids = detect_dict['old_ids']

                # there is also other checks to do
                # TODO check if all fields name are the same if updating dataset
                # TODO check if the number of columns is equal

                data_dict['method'] = 'update'
                data_dict['primary_key'] = current_entry_properties.primary_key

                updated_rows = detect_updated_rows(record_after_delete, old_record,
                                                   current_entry_properties.primary_key)

                insert_data = detect_inserted_rows(record_after_delete, old_ids, current_entry_properties.primary_key)

                data_dict['records'] = updated_rows

                print 'UPDATE_ROWS'
                print updated_rows

                super(VersionedDatastorePostgresqlBackend, self).upsert(context, data_dict)

                data_dict['method'] = 'insert'
                data_dict['records'] = insert_data

                t1 = time.time()
                delta = t1 - t0
                print 'DELTA'
                print delta
                total = total + delta
                print '==TOTAL=='
                print total
                return super(VersionedDatastorePostgresqlBackend, self).upsert(context, data_dict)
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

                data_dict['primary_key'] = current_entry_properties.primary_key
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
                create_versioning_trigger(data_dict, self.connection)
                create_op_type_trigger(identifier(data_dict['resource_id']),
                                       identifier(data_dict['resource_id'] + '_history'), self.connection)

                t1 = time.time()
                delta = t1 - t0
                print 'DELTA'
                print delta
                total = total + delta
                print '==TOTAL=='
                print total
                return result

    def delete(self, context, data_dict):
        raise NotImplementedError()

    def search(self, context, data_dict):
        data_dict_copy = data_dict
        connection = self.engine.connect()
        context['connection'] = connection
        validate(context, data_dict_copy)
        fields_types = _get_fields_types(
            context['connection'], data_dict_copy['resource_id'])
        query_dict = {
            'select': [],
            'sort': [],
            'where': []
        }

        query_dict = super(VersionedDatastorePostgresqlBackend, self).datastore_search(context, data_dict_copy,
                                                                                       fields_types, query_dict)

        query = convert_to_sql_query(query_dict, data_dict_copy)

        if is_query_needed(query):
            result = connection.execute(query)
            pid = QUERY_STORE.store_query(func.now(), query, hash_query(query), hash_query_result(result),
                                          data_dict_copy['resource_id'])
            initiliaze_pid(pid)

        connection.close()

        return super(VersionedDatastorePostgresqlBackend, self).search(context, data_dict)
