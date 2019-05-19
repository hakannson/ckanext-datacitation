import hashlib


def hash_query_result(result):
    result_string = ''
    for row in result:
        result_string += str(row)
    hash_object = hashlib.sha512(result_string.encode('utf-8'))
    hex_dig = hash_object.hexdigest()
    return hex_dig


def hash_query(query):
    hash_object = hashlib.sha512(query.encode('utf-8'))
    hex_dig = hash_object.hexdigest()
    return hex_dig



def convert_to_sql_query(query_dict, data_dict):
    records_format = data_dict['records_format']
    where_clause, where_values = _where(query_dict['where'])
    select_columns = ', '.join(query_dict['select']).replace('%', '%%')
    ts_query = query_dict['ts_query'].replace('%', '%%')
    resource_id = data_dict['resource_id'].replace('%', '%%')
    sort = query_dict['sort']
    limit = query_dict['limit']
    offset = query_dict['offset']
    if query_dict.get('distinct'):
        distinct = 'DISTINCT'
    else:
        distinct = ''
    if sort:
        sort_clause = 'ORDER BY %s' % (', '.join(sort)).replace('%', '%%')
    else:
        sort_clause = ''
    sql_fmt = u'''
                  SELECT * FROM (
                      SELECT {distinct} {select}
                      FROM "{resource}" {ts_query}
                      {where} {sort} LIMIT {limit} OFFSET {offset}
                  ) AS j'''
    replaced = replace_where_clause(where_clause, where_values)
    sql_string = sql_fmt.format(
        distinct=distinct,
        select=select_columns,
        resource=resource_id,
        ts_query=ts_query,
        where=replaced,
        sort=sort_clause,
        limit=limit,
        offset=offset)
    return sql_string


def _where(where_clauses_and_values):
    where_clauses = []
    values = []
    for clause_and_values in where_clauses_and_values:
        where_clauses.append('(' + clause_and_values[0] + ')')
        values += clause_and_values[1:]
    where_clause = u' AND '.join(where_clauses)
    if where_clause:
        where_clause = u'WHERE ' + where_clause
    return where_clause, values


def replace_where_clause(where_clause, where_values):
    replaced = str(where_clause)
    for value in where_values:
        replaced = replaced.replace("%s", "'" + value + "'", 1)
    return replaced


