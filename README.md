# pgpy
A module for completing basic PostgreSQL commands in a more pythonic way.

## Installation 
### Clone
Run the following command in your terminal:
```
git clone https://github.com/adnansiddiquei/pgpy
```

## Documentation
### Requirements
Using pgpy requires the following packages, which can all be installed via pip:
- psycopg2
- numpy
- pandas

### Getting started

This module works by creating object representations of each schema and table in your database. The object representations each have several methods which allow you to do basic PostgreSQL commands.

Start by importing the module like below.
```
import pgpy
```

### The database class
First, create a connection to your database:
```
db = pgpy.database(user='username123', password='password123', host='127.0.0.1', 
                    port='5432', database='postgres')
```

To view the metadata (all the schemas) for the database, use:
```
db.meta()
```

To create a new schema in your database:
```
db.create_schema('my_new_schema')
```

To close your database connection:
```
db.close()
```

### The schema class
To access the data contained inside any specific schema, to delete any schemas, or to create any new tables, you need to create a schema object from an existing schema inside your database. Let's assume you have a schema in your database called 'my_schema'.

To create the schema object that references an existing schema in your database, use:
```
my_schema = db['my_schema']
```

To access the metadata (schema name and table names) for this schema, you can use your newly created object, or direct indexing from the db object:
```
my_schema.meta()  # option 1
db['my_schema'].meta()  # option 2
```

To alter the name of the schema:
```
my_schema.rename('my_new_schema')  # will change the name of the schema to 'my_new_schema'
```

To delete the schema and all it's dependants, use:
```
my_schema.delete(cascade=True)
```

To create a new table from a pandas DataFrame (say, called 'new_table') inside this schema, use the following notation:
```
my_schema['new_table'] = new_table
```
If a table already exists inside the 'my_schema' schema called 'new_table' then it will be deleted and replaced by the new DataFrame.

### The table class
To access any data inside any tables, you need to create a table class:
```
my_table = db['my_schema']['my_table']  # option 1
my_table = my_schema['my_table']  # option 2, using the object we previously created
```

To access the metadata (table name, column names and their data types) for this table, use:
```
my_table.meta()
```

To access the data inside the table, you have a couple of options. The easiest way to retrieve the entire table or only a certain number of columns is:
```
my_table['*']  # this will return the entire table as a pandas DataFrame
my_table[['column1', 'column2', 'column5']]  # returns only these 3 columns
```

For more complex queries, use the select method. This method will return every column in the database subject to certain constraints, which are passed as an argument:
```
my_table.select()  # returns the entire table
my_table.select('WHERE column1 > 5 ORDER BY column3 ASC;')  # example of constraints
```
Do not inlcude the SELECT or FROM clauses in the select() method shown above. By deafult, this is set to 'SELECT * FROM my_schema.my_table'

To change the name of the table, alter the 'name' property:
```
my_table.rename('new_table_name')
```

To change the column names, alter the 'columns' propety by providing a new list of column names in the correct order, or a dict containing the old and new column names. If providing a list, then the list must be of the same length as the number of total columns. If you only wish to change one, or a few column names, then use option 2.
```
my_table.rename_columns(['new_column1_name', 'new_column2_name', 'new_column3_name'])  # option 1

# option 2
my_table.rename_columns({
    'column1': 'new_column1_name'
    'column3': 'new_column3_name'
})
```

To delete the table:
```
my_table.delete()
```

