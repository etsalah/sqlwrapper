"""
created on Jun 20, 2014
Last updated on September 29, 2015

@author: etsalah
@version: 0.0.6
"""
import os

CONNECTION_DETAILS = {
    'DBM_TYPE': 'MYSQL'
}

DEFAULT_MAX_ROWS = 0

SUPPORTED_RDBMS = ('MYSQL', 'SQLITE', 'POSTGRES')


def get_default_config():
    config_dict = {}
    for key in ['DB_HOST', 'DB_USERNAME', 'DB_PASSWORD', 'DB_NAME']:
        if os.environ.get(key):
            config_dict.update({key: os.environ[key]})

    CONNECTION_DETAILS.update(config_dict)

    return CONNECTION_DETAILS


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

    if not connection_config:
        connection_config = {}

    connection_config.update(get_default_config())
    dbm_type = connection_config['DBM_TYPE']

    if dbm_type not in SUPPORTED_RDBMS:
        raise NotImplementedError(
            "This library doesn't currently support the %s RDBMS" % dbm_type)

    try:
        if dbm_type == 'MYSQL':
            try:
                import MySQLdb
            except ImportError:
                import pymysql as MySQLdb

            my_db = MySQLdb

            conn = my_db.connect(
                connection_config['DB_HOST'], connection_config['DB_USERNAME'],
                connection_config['DB_PASSWORD'], connection_config['DB_NAME']
            )

            cursor = conn.cursor(my_db.cursors.DictCursor)

        elif dbm_type == 'SQLITE':
            import sqlite3 as lite
            db_file = connection_config.get('db_file', ':memory:')
            conn = lite.connect(db_file)
            conn.row_factory = lite.Row
            cursor = conn.cursor()

        else:
            raise NotImplementedError(
                "Add implementation details for %s" % dbm_type)
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
        parameters (Any): The list of parameters that need to by passed to
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
        query=None, parameters=(), connection_config=None):
    """This method is responsible for executing a query that is expected to
    return (a) value(s) to the caller
    Args:
        query(str): The query to be executed
        parameters(any): The parameters that need to be sent to execute along
            with the query
        connection_config(dict): the details to use to create a connection
    Returns:
        results(list[dict]):  A list of the results of the executions of the
            query represented as a list of dictionaries
    """
    conn = None
    try:
        conn, cursor = get_connection(connection_config)
        if parameters:
            cursor.execute(query, parameters)
        else:
            cursor.execute(query)
        return cursor.fetchall()
    except Exception as e:
        print('execute query \n~~~~~~~~~~~~~~~~\n%s' % e)
        raise e
    finally:
        if conn:
            conn.close()


def validate_limits(index, limit):
    """This method is used to validate if the index(offset) and limit provided
    for a query a valid or not
    Args:
        index(int): The number of rows to skip
        limit(int): The number of rows to return from the query result
    Returns:
        result(bool): Returns true if both the index(offset) and limit are
            valid. False if limit is zero or raises TypeError otherwise
    """
    result = True
    try:
        if index < 0:
            raise Exception("index can't be a negative number")

        if limit < 0:
            raise Exception("limit can't be a negative number")

        if limit == 0:
            raise Exception("limit can't be set to zero")

    except TypeError:
        raise Exception("Make sure that sure that index and limit are numbers")
    except Exception as e:
        raise e
    return result


def where_builder(
        index=0, limit=DEFAULT_MAX_ROWS, columns=(), order_by=('id desc',)):
    """This method is responsible for constructing the tail end (where and
    order by sections) of a query to be executed
    Args:
        index(int): The number of rows to skip
        limit(int): The number of rows to return from query
        columns(tuple): The list of columns that need to be in the where section
            of the query
        order_by(tuple): The list of columns plus their ordering (asc, desc) to
            put in the order by section of the query
    Returns:
    query(str): This is the resulting tail section of the query
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

    Args:
        cls_(callable): The name of the class of which objects need to be
            created
        results(list[dict]):  A list of results as a list of dictionaries
        columns(tuples): A tuple of column names (keys in the list of
            dictionaries) that also double as the name of fields in the class
    Returns:
        objects(list[Any]):  A list of instances of cls_ or an empty list
    """
    objects = []
    if len(results) > 0:
        for result in results:
            tmp = cls_()
            for column in columns:
                setattr(tmp, column, result.get(column, None))
            objects.append(tmp)
    return objects


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
        keys = tuple(args.keys())
        parameters = list(args.values())
        query += where_builder(0, 0, keys)
        records = execute_query(query, parameters, connection_config)
        results[column] = []
        for record in records:
            if record[column]:
                results[column].append(record[column])
    return results


def list_objects(table, cls, args=None, limits=None, connection_config=None):
    """This function is responsible for returning a list of rows, that match
    the arguments in args

    Args:
        table(str): name of the table or collection where data is currently
            saved
        cls(callable): The class whose instances should be created with the data
            returned from the database
        args(dict): column value pair to use to filter the data returned
        limits(dict): Arguments to use for pagination. Contains before an offset
            and a limit
        connection_config(dict): The connection details that can be passed into
            the function to use rather then the default that could be hard coded
    Returns:
        results(list[Any]): List of the instance of the class stored in cls
    """
    if not limits:
        limits = {}

    query = (
        "select * from %s %s" % (
            table, where_builder(
                limits.get('offset', 0),
                limits.get('limit', DEFAULT_MAX_ROWS),
                tuple(args.keys())
            )
        )
    )

    tmp_results = execute_query(query, list(args.values()), connection_config)
    return create_objects(cls, tmp_results, cls.COLUMNS)


def count_objects(table, args=None, connection_config=None):
    """This function is responsible for returning the number of objects that
    match the args that are passed in

    Args:
        table(str): The name of a table or view that we need to count objects
            from
        args(dict): The column, value pair to use to count the rows
        connection_config(dict): The details to use to connect to the database
    Returns:
        count(int)
    """
    if not args:
        args = {}

    query = (
        "select count(*) as row_count from %s %s" % (
            table, where_builder(0, 1, tuple(args.keys()))))
    results = execute_query(query, list(args.values()), connection_config)
    count = 0
    for row in results:
        count = row['row_count']
    return count
