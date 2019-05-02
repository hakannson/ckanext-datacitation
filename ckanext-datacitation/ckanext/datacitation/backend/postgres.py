from ckanext.datastore.backend.postgres import DatastorePostgresqlBackend,identifier, get_write_engine,_get_fields,_pluck,_get_fields_types,validate
import logging
from ckanext.datacitation.converter import hash_query_result,hash_query,convert_to_sql_query
from ckanext.datacitation.query_store import QueryStore
from sqlalchemy import func
from psycopg2.extras import DateTimeTZRange
from datetime import datetime
from ckanext.datacitation.postgresdb_controller import refine_results

import sys
reload(sys)
sys.setdefaultencoding('utf8')

log = logging.getLogger(__name__)

QUERY_STORE = QueryStore()


def create_op_type_trigger(table, table_history):
    engine = get_write_engine()
    connection = engine.connect()
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


def create_versioning_trigger(data_dict):
    engine = get_write_engine()
    connection = engine.connect()
    connection.execute(
        u'''CREATE TRIGGER {trigger}
            BEFORE INSERT OR UPDATE OR DELETE ON {table}
            FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period',
                                          '{table_history}',
                                          true);'''.format(
            trigger=identifier(data_dict['resource_id'] + '_trigger'), table=identifier(data_dict['resource_id']),
            table_history=identifier(data_dict['resource_id'] + '_history')))


def create_history_table(data_dict):
    columns = u", ".join([u'{0} {1}'.format(
        identifier(f['id']), f['type']) for f in data_dict['fields']])

    engine = get_write_engine()
    engine.execute(
        u' CREATE TABLE IF NOT EXISTS "{name}"({columns});'.format(
            name=data_dict['resource_id'],
            columns=columns
        )
    )


def find_diff(new_record, old_record_copy):
    diff = [i for i in old_record_copy + new_record if i not in old_record_copy or i not in new_record]
    return diff


def find_data_to_insert(diff, old_record):
    data_to_insert=[]
    for row in diff:
        if row not in old_record:
            data_to_insert.append(row)

    return data_to_insert

def find_deleted_data(diff,old_record):
    deleted_data=[]
    new_diff=[]
    for row in diff:
        if row in old_record:
            deleted_data.append(row)
        else:
            new_diff.append(row)

    return deleted_data,new_diff


def find_updated_data(diff,new_record,connection,table):
    new_diff=[]
    updated_ids=[]
    for dict in diff:
        if dict not in new_record:
            isFirst=True
            sql_string=''
            conditions = ''
            for key, value in dict.iteritems():
                if isFirst:
                    conditions+=key+'=' + "'" + value + "'"
                else:
                    conditions+=' AND ' + key+'=' + "'" + value+"'"
                isFirst=False
            sql_string += u'''SELECT _id FROM {table} WHERE {conditions}'''.format(table=identifier(table),conditions=conditions)
            row=connection.execute(sql_string).fetchone()
            if row is not None:
                updated_ids.append(row['_id'])
            else:
                new_diff.append(dict)

    return new_diff,updated_ids


def exclude_fields(old_record):
    for row in old_record:
        del row['_full_text']
        del row['sys_period']
        del row['_id']

    return old_record



def extract_column_names(fields):
    columns=[]
    for field in fields:
        columns.append(field.get('id',None))

    return columns



def update_table(rows,columns,records,connection,table):
    for id in rows:
        sql_string = u''' UPDATE "{res_id}"
                           SET ({columns}, "_full_text") = ({values}, NULL)
                           WHERE _id = {id};
                       '''.format(
           res_id=table,
           columns=u', '.join(
               [identifier(field)
                for field in columns]).replace('%', '%%'),
           values=u', '.join(
               ['%s' for _ in records]),
          id=id)
        print 'SQL_STRING'
        print sql_string
        connection.execute(sql_string)

def delete_items(deleted_data,table,connection):
    for dict in deleted_data:
        isFirst=True
        sql_string=''
        conditions = ''
        for key, value in dict.iteritems():
            if isFirst:
                conditions+=key+'=' + "'" + value + "'"
            else:
                conditions+=' AND ' + key+'=' + "'" + value+"'"
            isFirst=False
        sql_string += u'''SELECT _id FROM {table} WHERE {conditions}'''.format(table=identifier(table),conditions=conditions)
        row=connection.execute(sql_string).fetchone()
        delete_sql=u'''DELETE FROM {table} WHERE _id={condition};'''.format(table=identifier(table),condition=row['_id'])
        connection.execute(delete_sql)


def is_query_needed(query):
    if 'DISTINCT' in query:
        return False

    elif 'LIMIT 0 OFFSET 0' in query:
        return False
    else:
        return True

class VersionedDatastorePostgresqlBackend(DatastorePostgresqlBackend,object):

    def __init__(self):
        self.engine = get_write_engine()

    def create(self, context, data_dict):
        if super(VersionedDatastorePostgresqlBackend, self).resource_exists(data_dict['resource_id']):
            if not data_dict.get('records'):
                return
            connection = self.engine.connect()
            context['connection']=connection
            fields = _get_fields(context['connection'], data_dict['resource_id'])
            field_names = _pluck('id', fields)
            sql_columns = ", ".join(
                identifier(name) for name in field_names)



            select=u'''SELECT * FROM "{table}"'''.format(table=data_dict['resource_id'])
            rs = connection.execute(select)
            old_record = refine_results(rs, rs.keys())
            rs_copy=connection.execute(select)
            old_record_copy=exclude_fields(refine_results(rs_copy, rs_copy.keys()))
            new_record = data_dict['records']

            diff=find_diff(new_record,old_record_copy)

            print 'DIFF'
            print str(len(diff))
            print str(diff)

            diff,updated_ids=find_updated_data(diff,connection,data_dict['resource_id'])

            print 'NEW_DIFF_AFTER_UPDATE'
            print str(len(diff))
            print str(diff)


            data_to_delete, diff =find_deleted_data(diff,old_record_copy)

            print 'NEW_DIFF_AFTER_DELETE'
            print str(len(diff))
            print str(diff)

            print 'data_to_delete'
            print str(data_to_delete)

            data_to_insert=find_data_to_insert(diff, old_record_copy)

            delete_items(data_to_delete,data_dict['resource_id'],connection)

            insert_dict=data_dict


            insert_dict['records']= data_to_insert

            return super(VersionedDatastorePostgresqlBackend, self).create(context,insert_dict)

            #update_table(changed_row_ids,columns,new_record,connection,data_dict['resource_id'])

            #data_dict['records']=new_record

            #return super(VersionedDatastorePostgresqlBackend, self).create(context,data_dict)


        else:
            fields=data_dict.get('fields',None)
            records=data_dict.get('records',None)
            fields.append(
                {
                    "id": "sys_period",
                    "type": "tstzrange"
                }
            )

            if records is not None:
                for r in records:
                    r['sys_period']=DateTimeTZRange(datetime.now(),None)
            data_dict['fields']=fields
            data_dict['records']=records
            datastore_fields = [
                {'id': '_id', 'type': 'integer'},
                {'id': '_full_text', 'type': 'tsvector'},
            ]
            extra_field=[
                {
                    "id": "op_type",
                    "type": "text"
                }
            ]
            fields_of_history_table=datastore_fields + list(fields) + extra_field
            history_data_dict={
                "fields":fields_of_history_table,
                "resource_id":data_dict['resource_id']+'_history'
            }
            create_history_table(history_data_dict)
            result=super(VersionedDatastorePostgresqlBackend, self).create(context, data_dict)
            create_versioning_trigger(data_dict)
            create_op_type_trigger(identifier(data_dict['resource_id']),identifier(data_dict['resource_id']+'_history'))
            return result

    def delete(self, context, data_dict):
        print '========= DELETE method in Action ======='
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
            QUERY_STORE.store_query(func.now(), query, hash_query(query), hash_query_result(result), data_dict_copy['resource_id'])

        return super(VersionedDatastorePostgresqlBackend, self).search(context,data_dict)
