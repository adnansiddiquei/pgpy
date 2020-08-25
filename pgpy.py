""""
Module for connecting to postgreSQL database and doing common manipulation tasks.
Version 2.
"""

"""
NOTES:
- will throw and error when I initialise a table, then change the name of the schema that the table is in
    as the table is not aware of its parent schema's name change.
"""

import psycopg2 as pg2
from psycopg2.extras import execute_values
import numpy as np
import pandas as pd
from datetime import datetime, date, time
from itertools import chain

type_conversion = {int: 'int',  # used to convert between python data type and SQL data types
                   float: 'real',
                   str: 'text',
                   bool: 'bool',
                   datetime: 'timestamp',
                   date: 'date',
                   time: 'time',

                   np.float64: 'real',
                   np.int64: 'int',
                   np.int32: 'int',

                   pd._libs.tslibs.timestamps.Timestamp: 'timestamp'
                   }


def execute(database, query, return_values=True):
    """
    Executes a query and returns the results using a given database connection.

    :param database: database object.
    :param query: query to be executed.
    :param return_values: If true, this function will return the output of the query. Set to false to avoid an error when
    the query does not return anything.

    :return: query results.
    """
    cur = database.con.cursor()

    try:
        # execute query
        cur.execute(query)
        database.con.commit()
    except:
        # if there was an error, rollback and raise exception
        database.con.rollback()
        raise

    if return_values:
        rows = cur.fetchall()
        cur.close()
        return rows
    else:
        cur.close()
        return


class database:
    """
    A connection to a specified PostgresSQL Database.
    This class uses psycopg2 to provide functions to do common tasks
    """

    def __init__(self, user, password, host='127.0.0.1', port='5432', database='postgres'):
        """
        Initilise database class by connecting to a database.

        :param user: database user
        :param password: database password
        :param host: database host address, e.g. '127.0.0.1'
        :param port: database port number, e.g. '5432'
        :param database: database name, e.g. 'postgres'
        """
        self.con = pg2.connect(user=user,
                               password=password,
                               host=host,
                               port=port,
                               database=database)

    def __getitem__(self, item):
        """
        Retrieve an object representing a schema inside the database.

        :param item: name of schema.

        :return: object representing the schema.
        """

        if item in self.meta().keys():
            return schema(self, item)
        else:
            raise ValueError("The '{0}' schema does not exist.".format(item))

    def close(self):
        """
        Closes database connection.

        :return: None.
        """

        self.con.close()

    def meta(self):
        """
        :return: A dict of all schemas and tables in the database

            {
                schema1: [table1, table2, table3],
                schema2: [table1]
                ...
            }
        """

        query = """SELECT table_schema, table_name
                        FROM information_schema.tables
	                    WHERE "table_schema" != 'pg_catalog'
	                    AND "table_schema" != 'information_schema'
	                    ORDER BY "table_schema" ASC, "table_name" ASC;
                """

        rows = execute(self, query)  # execute above query
        # the output will be structured like [(table_schema, table_name), (table_schema, table_name)...]

        data = {}  # will store the schemas/tables

        for row in rows:
            # convert the 'rows' list into a dict
            if row[0] in data.keys():  # if the schema is already in the 'data' dict
                data[row[0]].append(row[1])  # append the new table onto the dict
            else:
                data[row[0]] = [row[1]]

        return data

    def create_schema(self, table_schema):
        """
        Creates a schema in this database using:
            CREATE SCHEMA table_schema;

        :param table_schema: name of schema to create

        :return: None.
        """
        query = 'CREATE SCHEMA "{0}";'.format(table_schema)
        execute(self, query, return_values=False)


class schema:
    """
    An object representing a connection to a specific schema.
    """

    def __init__(self, database, table_schema):
        """
        Initilialises schema object.

        :param database: database object.
        :param table_schema: schema name.
        """
        self.database = database
        self._table_schema = table_schema

    def __getitem__(self, item):
        """
        Retrieve an object representing a table inside this schema.

        :param item: The table name.

        :return: object representing the table.
        """

        if item in self.meta().keys():
            return table(self.database, self.table_schema, item)
        else:
            raise ValueError("The '{0}' table does not exist.".format(item))

    def __setitem__(self, key, value):
        """
        Create a new table in this schema from a DataFrame. If a table with the same name already exists then it will overwrite it.

        :param key: name of table
        :param value: DataFrame to insert into the database

        :return: None
        """

        if key in self.meta().keys():  # if table already exists then delete it
            self[key].delete()

        dataframe = value.copy()

        # if dataframe.index is anything other than the standard numeric, then include it in the table
        if type(dataframe.index) != pd.core.indexes.range.RangeIndex:
            old_column_order = list(dataframe.columns)
            dataframe['Index'] = dataframe.index
            new_column_order = list(chain(['Index'], old_column_order))
            dataframe = dataframe[new_column_order]

        # now the dataframe has been re-shaped so that the index column is in dataframe.values

        column_and_datatype_string = []
        column_string = []
        for col in dataframe.columns:
            first_valid_index = dataframe[col].first_valid_index()
            first_valid_value = dataframe[col].loc[first_valid_index]
            data_type = type_conversion[type(first_valid_value)]

            column_and_datatype_string.append('"{0}" {1}'.format(col, data_type))
            column_string.append('"{0}"'.format(col))

        column_and_datatype_string = '{0}'.format(', '.join(column_and_datatype_string))
        # stores '(column_name column_type, column_name column_type...)'

        query = 'CREATE TABLE "{0}"."{1}" ({2}); '.format(self.table_schema, key, column_and_datatype_string)
        execute(self.database, query, return_values=False) # create table

        #replace any nulls with None
        dataframe = pd.DataFrame(np.where(dataframe.isnull() == True, None, dataframe),
                                 columns=dataframe.columns)

        query = 'INSERT INTO "{0}"."{1}" ({2}) VALUES %s'.format(self.table_schema, key, ', '.join(column_string))

        try:
            # execute query
            execute_values(self.database.con.cursor(),
                           query,
                           dataframe.values)
            self.database.con.commit()
        except:
            # if there was an error, rollback and raise exception
            self.database.con.rollback()
            raise

    def meta(self):
        """
        :return: A dict of all tables and their columns inside the database.

            {
                table1: [col1, col2, col33],
                table2: [col1, col2]
                ...
            }
        """

        query = """SELECT table_name, column_name
                        FROM information_schema.columns
                        WHERE table_schema = '{0}'
                        ORDER BY table_name ASC, ordinal_position ASC;
                """.format(self.table_schema)

        rows = execute(self.database, query)  # execute above query
        # the output will be structured like [(table_name, column_name), (table_name, column_name)...]

        data = {}  # will store the tables/columns

        for row in rows:
            # convert the 'rows' list into a dict
            if row[0] in data.keys():  # if the schema is already in the 'data' dict
                data[row[0]].append(row[1])  # append the new table onto the dict
            else:
                data[row[0]] = [row[1]]

        return data

    @property
    def table_schema(self):  # retrieve schema name
        return self._table_schema

    @property
    def name(self):  # retrieve schema name
        # this does the same as above but 'name' is more intuitive for the user while 'table_schema'
        # will be used for anything in the code
        return self._table_schema

    @name.setter
    def name(self, new_name):  # alters the schema name when this value is altered
        query = 'ALTER SCHEMA "{0}" RENAME TO "{1}";'.format(self.name, new_name)
        execute(self.database, query, return_values=False)
        self._table_schema = new_name

    def delete(self, cascade=False):
        """
        Deletes the current scheme instance in the database using:
            DROP SCHEMA table_schema;
                or
            DROP SCHEMA table_schema CASCADE;

        :param cascade: set to 'True' to include CASCADE in the query.

        :return: None.
        """

        if cascade == True:
            query = 'DROP SCHEMA "{0}" CASCADE;'.format(self.table_schema)
            execute(self.database, query, return_values=False)
        elif cascade == False:
            query = 'DROP SCHEMA "{0}";'.format(self.table_schema)
            execute(self.database, query, return_values=False)
        else:
            raise ValueError("Argument 'cascade' needs to be either True or False.")



class table:
    """
    An object representing a connection to a specified table
    """

    def __init__(self, database, table_schema, table_name):
        self.database = database
        self.table_schema = table_schema
        self._table_name = table_name
        self._columns = list(self.meta().keys())

    def __getitem__(self, item):
        # item = column name or list of column names

        columns = self.meta().keys()  # a list of all columns in the database

        is_iterable = (type(item) == list) | (type(item) == tuple)  # checks if a list of columns was entered

        # check if any of the specified columns don't exist in the table
        if is_iterable:
            if not all([i in columns for i in item]):  # i.e. if a specified column doesn't exist
                raise ValueError("One of the specified columns doesn't exist in the table.")
        else:
            if (not (item in columns)) & (item != '*'):
                raise ValueError("The specified column, '{0}', doesn't exist in the table.".format(item))

            item = [item]  # convert to list so that ', '.join(item) works later on

        if item != ['*']:
            query = 'SELECT {0} FROM "{1}"."{2}"'.format('"' + '", "'.join(item) + '"',
                                                         self.table_schema,
                                                         self.table_name)
        else:
            query = 'SELECT * FROM "{0}"."{1}"'.format(self.table_schema, self.table_name)

        rows = execute(self.database, query)

        data = pd.DataFrame(rows)

        if item == ['*']:
            data.columns = columns  # use the ordered meta().keys() as the column headers
        else:
            data.columns = item  # use the users input

        return data

    @property
    def table_name(self):
        return self._table_name

    @property
    def name(self):
        return self._table_name

    @name.setter
    def name(self, new_name):  # alters the table name when this value is altered
        query = 'ALTER TABLE "{0}"."{1}" RENAME TO "{2}";'.format(self.table_schema, self.name, new_name)
        execute(self.database, query, return_values=False)
        self._table_name = new_name

    @property
    def columns(self):
        return self._columns

    @columns.setter
    def columns(self, new_columns):
        """
        Changes the column names of the database using a dict or list.

        :param new_columns: The new columns in the form of a dict or a list. The list must contain the same number of
            column names as the original database table. The dict must be so that the keys are the old column names and
            the values are the new ones

        :return: None
        """
        old_columns = list(self.meta().keys())

        query = ''

        if (type(new_columns) == list) | (type(new_columns) == tuple):
            for old_column, new_column in zip(old_columns, new_columns):
                if old_column == new_column:  # skip if the new name is identical to the old one
                    continue

                query += 'ALTER TABLE "{0}"."{1}" RENAME COLUMN "{2}" TO "{3}"; '.format(
                    self.table_schema, self.table_name, old_column, new_column
                )



            if query == '':  # i.e. if all the old column names are the same as the new ones
                return
        elif type(new_columns) == dict:
            for old_column, new_column in zip(list(new_columns.keys()), list(new_columns.values())):
                if old_column not in old_columns:
                    raise ValueError('"{0}" is not an existing column in this table'.format(old_column))

                query += 'ALTER TABLE "{0}"."{1}" RENAME COLUMN "{2}" TO "{3}"; '.format(
                    self.table_schema, self.table_name, old_column, new_column
                )

        execute(self.database, query, return_values=False)

    def meta(self):
        """
        :return: A dict containing the column names and their data type, in order of ordinal position.

                {
                    column1: data_type,
                    column2: data_type,
                    ...
                }
        """

        query = """SELECT column_name, data_type 
                        FROM information_schema.columns
                        WHERE table_schema = '{0}'
                        AND table_name = '{1}'
                        ORDER BY ordinal_position ASC;
                """.format(self.table_schema, self.table_name)

        rows = execute(self.database, query)  # execute above query
        # the output will be structured like [(columns_name, data_type), (column_name, data_type)...]

        data = {}  # will store the column/data type

        for row in rows:
            data[row[0]] = row[1]

        return data

    def delete(self):
        """
        Delete table from database using:
            DROP TABLE table_schema.table_name;

        :return: None.
        """

        query = 'DROP TABLE "{0}"."{1}";'.format(self.table_schema, self.table_name)
        execute(self.database, query, return_values=False)

    def select(self, conditions=''):
        """
        Create a custom query on a table. Include all the text following: 'SELECT * FROM schema.table'...

        :param conditions: A string consisting of the WEHRE, ORDER BY (etc.) clauses.

        :return: A dataframe of the results of the query.
        """

        columns = list(self.meta().keys())
        query = 'SELECT {3} FROM "{0}"."{1}" {2}'.format(self.table_schema, self.table_name, conditions,
                                                         '"' + '", "'.join(columns) + '"')

        if conditions == '':
            query += ';'

        rows = execute(self.database, query)

        data = pd.DataFrame(rows, columns=columns)
        return data