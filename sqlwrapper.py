"""
created on Jun 20, 2014
Last updated on September 29, 2015

@author: etsalah
@version: 0.0.6
"""
import os

CONNECTION_DETAILS = {
    'db_type': 'MYSQL', 'host': os.environ['DB_HOST'],
    'username': os.environ['DB_USERNAME'],
    'password': os.environ['DB_PASSWORD'],
    'database': os.environ['DB_NAME']}

DEFAULT_MAX_ROWS = 0

SUPPORTED_RDBMS = ('MYSQL', 'SQLITE')


def get_connection(connection_config=None):
    """This method is responsible for creating a connection object

    Args:
        connection_config (dict): details for creating a connection for
            the application

    Returns:
        conn: a connection object if a connection was established or None
        cursor: a cursor object to use for making calls to the database

    Raises:
        NotImplementedError: if the db type that is specified is currently
            not supported
        Exception: if an error occurrs during the connection process
    """
    if connection_config:
        CONNECTION_DETAILS.update(connection_config)

    db_type = CONNECTION_DETAILS.get('db_type', None)

    if db_type not in SUPPORTED_RDBMS:
        raise NotImplementedError(
            "This library doesn't currently support the %s RDBMS" % db_type)

    try:
        if db_type == 'MYSQL':
            import MySQLdb
            my_db = MySQLdb

            conn = my_db.connect(
                CONNECTION_DETAILS['host'], CONNECTION_DETAILS['username'],
                CONNECTION_DETAILS['password'],
                CONNECTION_DETAILS['database'])

            cursor = conn.cursor(my_db.cursors.DictCursor)

        elif db_type == 'SQLITE':
            import sqlite3 as lite
            db_file = CONNECTION_DETAILS.get('db_file', ':memory:')
            conn = lite.connect(db_file)
            conn.row_factory = lite.Row
            cursor = conn.cursor()

        else:
            raise NotImplementedError(
                "Add implementation details for %s" % db_type)
    except Exception as e:
        print('connection \n~~~~~~~~~~~\n%s' % e)
        raise e
    return conn, cursor


def validate(connection=None, query=None):
    if not connection:
        raise Exception(
            "You need to provide the connection object to the database")

    if not query:
        raise Exception(
            "You need to provide the query to be executed against the database")


def execute_non_query(query=None, parameters=None, connection_config=None):
    """This method is responsible for executing a query that returns no records
    and return True if it succeeded or false otherwise
    Args:
        query (str): The query to be executed
        parameters (dict): The list of parameters that need to by passed to
            execute command
        connection_config (dict): The configuration to get a connection to the
            db
    Returns:
        result (bool): Returns true if query was executed successfully false
            otherwise
    Raises:
        Exception: when any error occurred during the execution of query
    """

    if not parameters:
        parameters = {}
    conn = None
    try:
        conn, cursor = get_connection(connection_config)
        if parameters:
            cursor.execute(query, parameters)
        else:
            cursor.execute(query)
        conn.commit()
        result = True
    except Exception as e:
        print(e)
        raise e
    finally:
        if conn:
            conn.close()
    return result


def execute_query(
    query=None, columns=(), parameters=(), connection_config=None):
    """This method is responsible for executing a query that is expected to
    return (a) value(s) to the caller
    ~~~~~~
    arg(s)
    ~~~~~~
    query       ->  The query to be executed
    columns     ->  The columns to be returned from the query if successful
    parameters  ->  The parameters that need to be sent to execute along with
                    the query
    ~~~~~~~~~
    return(s)
    ~~~~~~~~~
    results     ->  A list of the results of the executions of the query
                    represented as a list of dictionaries
    """
    results = []
    conn = None
    try:
        conn, cursor = get_connection(connection_config)
        if parameters:
            cursor.execute(query, parameters)
        else:
            cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            obj_ = {}
            for column in columns:
                obj_[column] = row[column]
            results.append(obj_)
    except Exception as e:
        print('execute query \n~~~~~~~~~~~~~~~~\n%s' % e)
        raise e
    finally:
        if conn:
            conn.close()
    return results


def validate_limits(index, limit):
    """This method is used to validate if the index(offset) and limit provided
    for a query a valid or not
    ~~~~~~
    arg(s)
    ~~~~~~
    index(offset)   ->  The number of rows to skip
    limit           ->  The number of rows to return from the query result
    ~~~~~~
    return
    ~~~~~~
    return          ->  Returns true if both the index(offset) and limit are
                        valid. False if limit is zero or raises TypeError
                        otherwise
    """
    result = True
    try:
        if index < 0:
            raise Exception("index can't be a negative number")

        if limit < 0:
            raise Exception("limit can't be a negative number")

        if limit == 0:
            result = False

    except TypeError:
        raise Exception("Make sure that sure that index and limit are numbers")
    except Exception as e:
        raise e
    return result


def where_builder(
    index=0, limit=DEFAULT_MAX_ROWS, columns=(), order_by=('id desc',)):
    """This method is responsible for constructing the tail end (where and
    order by sections) of a query to be executed
    ~~~~~~
    arg(s)
    ~~~~~~
    index(offset)   ->  The number of rows to skip
    limit           ->  The number of rows to return from query
    columns         ->  The list of columns that need to be in the where section
                        of the query
    order_by        ->  The list of columns plus their ordering (asc, desc) to
                        put in the order by section of the query
    ~~~~~~~~~
    return(s)
    ~~~~~~~~~
    query           ->  This is the resulting tail section of the query
    """
    and_ = False
    pieces = ()
    if len(columns) > 0:
        query = ' where'
        for column in columns:
            pieces += (" %s = %s" % (column, '%s'),)

        for piece in pieces:
            if not and_:
                query += piece
                and_ = True
            else:
                query += (" and" + piece)

    else:
        query = ''

    ever_ordered_by = False
    for order in order_by:
        if not ever_ordered_by:
            query += ' order by '
            ever_ordered_by = True
        else:
            query += ','
        query += ('%s' % order)

    limit = DEFAULT_MAX_ROWS if not limit else limit
    if validate_limits(index, limit):
        query += (" limit %d, %d" % (index, limit))
    return query


def create_objects(cls_, results, columns=()):
    """This method is responsible for creating a list of objects from the class,
    results and columns that are passed to it.
    ~~~~~~
    arg(s)
    ~~~~~~
    cls_        ->  The name of the class of which objects need to be created
    results     ->  A list of results as a list of dictionaries
    columns     ->  A tuple of column names (keys in the list of dictionaries)
                    that also double as the name of fields in the class
    ~~~~~~~~~
    return(s)
    ~~~~~~~~~
    objs         -> A list of instances of cls_ or an empty list
    """
    objs = []
    if len(results) > 0:
        for result in results:
            tmp = cls_()
            for column in columns:
                setattr(tmp, column, result.get(column, None))
            objs.append(tmp)
    return objs


def get_filters(table, columns=(), args=None, connection_config=None):
    """This method is responsible for returning distinct values for the columns
    that are passed to it
    ~~~~~~
    arg(s)
    ~~~~~~
    table       ->  The name of the table from which the values need to be
                    returned
    columns     ->  A list of columns where the distinct values need to be
                    returned from
    args        ->  A dictionary that contains name of the field to use to
                    restrict the returned list as key and value as value of the
                    dictionary
    ~~~~~~~~~
    return(s)
    ~~~~~~~~~
    results     ->  A dictionary that represents the results. The column is the
                    key in the dictionary and a list of distinct values are the
                    value of the dictionary
    """
    results = {}
    if not args:
        args = {}

    for column in columns:
        query = 'select distinct(%s) from %s' % (column, table)
        keys = args.keys()
        parameters = args.values()
        query += where_builder(0, 0, keys)
        records = execute_query(query, (column,), parameters, connection_config)
        results[column] = []
        for record in records:
            if record[column]:
                results[column].append(record[column])
    return results
