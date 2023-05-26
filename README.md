



#  GRANTS SQL Script Generator : generate_role_grants_util.py

This project provides a set of utility functions to generate SQL script files for common tasks related to database management. The functions are designed to simplify the process of generating SQL scripts for creating roles, granting roles and privileges, and managing database objects.

## Installation

To use this project, you need to have Python installed. You can install the required dependencies by running the following command:

 &nbsp;  pip install  snowflake-connector

&nbsp; pip install  deprecated


## Usage

### 1. Connect to the database:

```python
import snowflake.connector

## Replace the placeholders with your actual database connection details
conn=snowflake.connector.connect(
                user=USER_NAME,
                account=ACCOUNT,
                warehouse=WAREHOUSE,
                database=SNOW_DATABASE,
                schema=SNOW_SCHEMA,
                authenticator='externalbrowser',
                role='ACCOUNTADMIN'
                )
```

&nbsp;



### 2. Generate SQL script for creating roles:

```python
from generate_role_grants_util import generate_create_roles_file

generate_create_roles_file(conn, "ACCOUNT", "create_roles.sql", generate_create_role_queries)
```

This function generates a SQL script file named "create_roles.sql" that contains the queries to create roles in the "ACCOUNT" schema.
The `generate_create_role_queries` function is responsible for generating the necessary SQL queries to create the roles.

&nbsp;
### 3. Generate SQL script for granting roles and hierarchy:

```python
from generate_role_grants_util import generate_create_roles_file

generate_create_roles_file(conn, "ACCOUNT", "create_role_hierarchy.sql", generate_grant_role_queries)
```

This function generates a SQL script file named "create_role_hierarchy.sql" that contains the queries to grant roles and their hierarchy in the "ACCOUNT" schema. 
The `generate_grant_role_queries` function is responsible for generating the necessary SQL queries to grant the roles and their hierarchy.

&nbsp;
### 4. Generate SQL script for assigning account privileges:

```python
from generate_role_grants_util import generate_create_roles_file

generate_create_roles_file(conn, "ACCOUNT", "account_privileges_queries.sql", generate_assign_account_privileges_queries)
```

This function generates a SQL script file named "account_privileges_queries.sql" that contains the queries to assign account privileges in the "ACCOUNT" schema. 
The `generate_assign_account_privileges_queries` function is responsible for generating the necessary SQL queries to assign the privileges.

&nbsp;
### 5. Generate SQL script for object future grant queries:

```python
from generate_role_grants_util import generate_object_future_grant_queries

generate_object_future_grant_queries(conn, 'queries', 'DATABASE', 'YourDatabase', 'SCHEMA', True)
```

This function generates a SQL script file named "queries.sql" that contains the queries to grant future privileges on objects in the "DATABASE" and "SCHEMA". 
The script enables future grants on objects and ensures that any new objects created in the specified database and schema will inherit the granted privileges.

&nbsp;
### 6. Generate SQL script for existing grant queries:

```python
from generate_role_grants_util import generate_object_grant_queries

generate_object_grant_queries(conn, 'queries', 'DATABASE', 'YourDatabase', 'SCHEMA', True, False)
```

This function generates a SQL script file named "queries.sql" that contains the queries to grant existing privileges on objects in the "DATABASE" and "SCHEMA". 
The script grants privileges on existing objects in the specified database and schema. If Recursive flag is true it will create a recursive folder hierarchy with grants of all objects.

&nbsp;
## Contributing

Contributions are welcome! If you have any suggestions, bug reports, or feature requests, please open an issue on the GitHub repository.

&nbsp;
## Authors

- [@TiMusBhardwaj](https://www.github.com/TiMusBhardwaj)

&nbsp;
## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more information.
```

Feel free to modify the content and formatting of the README to match your preferences.
