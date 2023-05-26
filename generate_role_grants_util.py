import snowflake.connector
import os
from deprecated import deprecated
####PARAMS

AUTHMECHANISM='SCRAM-SHA-1'
DATABASE='MyCompany'
ACCOUNT='MyCompanydev'
WAREHOUSE=
SNOW_DATABASE='MyCompany'
SNOW_SCHEMA='STAGE_SP'
excluded_roles = ['ACCOUNTADMIN', 'SYSADMIN', 'SECURITYADMIN', 'USERADMIN','APPADMIN', 'WORKSHEETS_APP_RL' ]

def generate_object_future_grant_queries(conn, parent_folder_name, parent_object_type, parent_object_name, object_type, recursive, included_objects=None):
    # Fetch the databases in the specified Snowflake account
    
    """
    Generates Snowflake SQL queries for granting future privileges on objects within a Snowflake database.

    Args:
        conn (connection): Connection object to the Snowflake database.
        parent_folder_name (str): Name of the parent folder to store the generated queries.
        parent_object_type (str): Type of the parent object. Possible values: DATABASE, SCHEMA.
        parent_object_name (str): Name of the parent object.
        object_type (str): Type of the objects to fetch future grants for.
        recursive (bool): Flag indicating whether to generate future grants for child objects recursively.
        included_objects (list, optional): List of specific objects to include. Default is None, which includes all objects.

    Raises:
        SnowflakeException: If there is an error executing the Snowflake queries.

    Returns:
        None
    """
    cur = conn.cursor()
    cur.execute(f"SHOW {object_type}S IN {parent_object_type} {parent_object_name}")
    
    objects = [row[1] for row in cur.fetchall()]
    if included_objects:
        objects = [obj for obj in included_objects if obj in included_objects]
    
    objects = ['"' + obj + '"' if not obj.isupper() else obj for obj in objects]   
    print(objects)
    

    # Iterate over the databases
    for obj in objects:
        # Fetch future grants in the current database
        cur.execute(f"SHOW FUTURE GRANTS IN {object_type} {parent_object_name}.{obj}")
        grants = cur.fetchall()
       
        # Create a folder for the current database
        folder = os.path.join(parent_folder_name, obj)
        if len(grants) >0:
            if not os.path.exists(folder):
                os.makedirs(folder)

            # Write future grant queries to a file in the database folder
            file_path = os.path.join(folder, "future_grants.sql")
            with open(file_path, 'w') as file:
                for grant in grants:
                    created_on, privilege, granted_on, name, granted_to, grantee_name,grant_option = grant
                    query = f"GRANT {privilege} ON FUTURE {granted_on}S IN  {object_type} {parent_object_name}.{obj} TO ROLE {grantee_name} "
                    if grant_option == 'Y':
                        query += " WITH GRANT OPTION"
                    query += ";"
                    file.write(query)
                    file.write('\n')
        if object_type == 'DATABASE' and recursive:
            generate_object_future_grant_queries(conn, folder, 'DATABASE', parent_object_name+'.'+obj, 'SCHEMA', False, None)   
    cur.close()

    print(f"Future grant queries have been written to {folder}.")


def generate_object_grant_queries(conn, parent_folder_name, parent_object_type, parent_object_name, object_type, recursive, single_file, included_objects=None):
    
    """
    Generates Snowflake SQL queries for granting privileges on objects within a Snowflake database.
    

    Args:
        conn (connection): Connection object to the Snowflake database.
        parent_folder_name (str): Name of the parent folder to store the generated queries.
        parent_object_type (str): Type of the parent object. Possible values: DATABASE, SCHEMA, None.
        parent_object_name (str): Name of the parent object. Empty string for fetching all objects of the specified type.
        object_type (str): Type of the objects to fetch and generate grants for.
        recursive (bool): Flag indicating whether to generate grants for child objects recursively.
        single_file (bool): Flag indicating whether to generate grants for each object in a separate file.
        included_objects (list, optional): List of specific objects to include. Default is None, which includes all objects.

    Raises:
        SnowflakeException: If there is an error executing the Snowflake queries.

    Returns:
        None
    """

    
    print(parent_folder_name, parent_object_type, parent_object_name, object_type)
    try:
        # Fetch objects of the specified type within the parent object
        cur = conn.cursor()
        if parent_object_type:
            cur.execute(f"SHOW {object_type}S IN {parent_object_type} {parent_object_name}")
        else:
            cur.execute(f"SHOW {object_type}S ")
            
        if (object_type in ['USER FUNCTION', 'USER PROCEDURE']):
            cur.execute('select "arguments" from table(RESULT_SCAN(last_query_id()));')
            object_type = object_type.split('USER ')[1]
            objects = [x[0].split('RETURN')[0] for x in cur.fetchall()]
        else:    
            cur.execute('select "name" from table(RESULT_SCAN(last_query_id()));')
            objects = [x[0] for x in cur.fetchall() ]
            
        if included_objects:
            objects = [obj for obj in included_objects if obj in included_objects]
        objects = ['"' + obj + '"' if not obj.isupper() else obj for obj in objects]
        print(objects)
        # Create a folder for the parent object if it doesn't exist
        #folder_name = parent_object_name.replace(' ', '_')
        folder_name = parent_folder_name #os.path.join(parent_folder_name,folder_name)
        if not os.path.exists(folder_name):
            os.makedirs(folder_name)

        # Generate grant queries for each object
        for obj_name in objects:
            
            
            if parent_object_name:
                obj_name = f"{parent_object_name}.{obj_name}"
           
            print(obj_name)
            cur.execute(f"SHOW GRANTS ON {object_type} {obj_name}")    
            grants = cur.fetchall()
            if  single_file:
                folder_name_obj=folder_name
            else:    
                folder_name_obj = os.path.join(folder_name,obj_name)
            if not os.path.exists(folder_name_obj):
                os.makedirs(folder_name_obj)
            
            # Generate the file path for the object's grant queries
            file_path = os.path.join(folder_name_obj, f"{object_type}_grants.sql")

            # Write the grant queries to the file
            with open(file_path, 'a') as file:
                for grant in grants:
                    created_on, privilege, granted_on, name, granted_to, grantee_name, grant_option, granted_by = grant
                    if grantee_name not in excluded_roles:
                        query = f"GRANT {privilege} ON {granted_on} {obj_name} TO ROLE {grantee_name}"
                        if grant_option == 'Y':
                            query += " WITH GRANT OPTION"
                        query += ";"
                        file.write(query)
                        file.write('\n')

            
            if object_type == 'DATABASE' and recursive:
                generate_object_grant_queries(conn, folder_name_obj, object_type, obj_name, "SCHEMA", True, False)
            if object_type == 'SCHEMA' and recursive:
                generate_object_grant_queries(conn, folder_name_obj, object_type, obj_name, "TASK", False, True)
                generate_object_grant_queries(conn, folder_name_obj, object_type, obj_name, "STAGE", False, True)
                generate_object_grant_queries(conn, folder_name_obj, object_type, obj_name, "USER FUNCTION", False, True)
                generate_object_grant_queries(conn, folder_name_obj, object_type, obj_name, "USER PROCEDURE", False, True)
                generate_object_grant_queries(conn, folder_name_obj, object_type, obj_name, "TABLE", False, True)
                generate_object_grant_queries(conn, folder_name_obj, object_type, obj_name, "VIEW", False, True)
                generate_object_grant_queries(conn, folder_name_obj, object_type, obj_name, "PIPE", False, True)
                generate_object_grant_queries(conn, folder_name_obj, object_type, obj_name, "STREAM", False, True)
                generate_object_grant_queries(conn, folder_name_obj, object_type, obj_name, "SEQUENCE", False, True)
                generate_object_grant_queries(conn, folder_name_obj, object_type, obj_name, "FILE FORMAT", False, True)
            
        print(f"Grant queries for {object_type}s in {parent_object_name} have been written to files in the folder: {folder_name}")

    finally:
        if cur:
            cur.close()  # Close the cursor

def generate_grant_database_queries_all(conn):
    
    """
    Generate grant queries for all roles in a database, excluding system roles.
    //TODO: Needs some work will give wrong results for FUNCTIONS and Procedures

    Parameters:
        conn (Connection): The connection object used to connect to the database.
        excluded_roles (list, optional): A list of roles to be excluded from the grant generation process.
    
    Returns:
        list: A list of grant queries generated for all roles in the database.

    Raises:
        None

    Example Usage:
        conn = create_database_connection()  # Example function to create a database connection
        grants = generate_grant_database_queries_all(conn)
        for query in grants:
            print(query)
    """
   

    try:
        # Fetch all roles excluding system roles
        cur = conn.cursor()
        cur.execute("SHOW ROLES")
        roles = [row[1] for row in cur.fetchall() if row[1] not in excluded_roles ]

        # Generate grant queries for each role
        queries = []
        for role in roles:
            cur.execute(f"SHOW GRANTS TO ROLE {role}")
            grants = [row for row in cur.fetchall() if row[2] not in ['ROLE']]
            for grant in grants:
                created_on, privilege, granted_on, name, granted_to, grantee_name, grant_option, granted_by = grant
                query = f"GRANT {privilege} ON {granted_on} {name} TO ROLE {grantee_name}"
                if grant_option == 'Y':
                    query += " WITH GRANT OPTION"
                query += ";"
                queries.append(query)

        return queries

    finally:
        if cur:
            cur.close()  # Close the cursor

@deprecated            
def generate_snowflake_object_grant_queries(conn, parent_object_type, parent_object_name, object_type):
    
    """
    Generates grant queries for objects of a specified type within a parent object in Snowflake.
    TODO: needs work, dont work for PROCEDURE AND FUNCTIONS
    Parameters:
    - conn (Connection): The connection object used to connect to the Snowflake database.
    - parent_object_type (str): The type of the parent object (e.g., 'DATABASE', 'SCHEMA').
    - parent_object_name (str): The name of the parent object.
    - object_type (str): The type of the objects to generate grant queries for (e.g., 'TABLE', 'VIEW').
    
    Returns:
    - queries (list): A list of grant queries generated for the specified objects.
    
    Notes:
    - This function retrieves the objects of the specified type within the parent object using the provided connection object.
    - It then generates grant queries for each object by fetching the corresponding grants using the SHOW GRANTS statement.
    - The generated grant queries are returned as a list.
    """

    try:
        # Fetch objects of the specified type in the specified schema
        cur = conn.cursor()
        cur.execute(f"SHOW {object_type}s IN {parent_object_type} {parent_object_name}")
        objects = cur.fetchall()

        # Generate grant queries for each object
        queries = []
        for obj in objects:
            obj_name = obj[1]
            cur.execute(f"SHOW GRANTS ON {object_type} {parent_object_name}.{obj_name}")
            grants = cur.fetchall()
            for grant in grants:
                created_on, privilege, granted_on, name, granted_to, grantee_name, grant_option, granted_by = grant
                if granted_to not in excluded_roles:
                    query = f"GRANT {privilege} ON {granted_on} {name} TO ROLE {grantee_name}"
                    if grant_option == 'Y':
                        query += " WITH GRANT OPTION"
                    query += ";"
                    queries.append(query)

        return queries

    finally:
        if cur:
            cur.close()  # Close the cursor

@deprecated     
def generate_snowflake_object_grant_queries(conn, object_type, object_name):
    
    """
    Generates grant queries for a specific object in Snowflake.
    TODO: needs work, dont work for PROCEDURE AND FUNCTIONS
    Parameters:
    - conn (Connection): The connection object used to connect to the Snowflake database.
    - object_type (str): The type of the object (e.g., 'DATABASE', 'SCHEMA', 'TABLE').
    - object_name (str): The name of the object.
    
    Returns:
    - queries (list): A list of grant queries generated for the specified object.
    
    Notes:
    - This function retrieves the grants for the specified object using the SHOW GRANTS statement.
    - It generates grant queries for each grant by iterating over the fetched grants.
    - The generated grant queries are returned as a list.
    """
    try:
        # Fetch grants on the specified database
        cur = conn.cursor()
        cur.execute(f"SHOW GRANTS ON {object_type} {object_name}")
        grants = cur.fetchall()

        # Generate grant queries for each grant
        queries = []
        
        for grant in grants:
            created_on, privilege, granted_on, name, granted_to, grantee_name, grant_option, granted_by = grant
            if granted_to not in excluded_roles:
                query = f"GRANT {privilege} ON {granted_on} {name} TO ROLE {grantee_name}"
                if grant_option == 'Y':
                    query += " WITH GRANT OPTION"
                query += ";"
                queries.append(query)

        return queries

    finally:
        if cur:
            cur.close()  # Close the cursor

def generate_assign_account_privileges_queries(conn):
    
    """
    Generates ASSIGN PRIVILEGES queries for account privileges in Snowflake.
    
    Parameters:
    - conn (Connection): The connection object used to connect to the Snowflake database.
    
    Returns:
    - queries (list): A list of ASSIGN PRIVILEGES queries generated for the account privileges.
    
    Notes:
    - This function fetches the grants on the account using the SHOW GRANTS statement.
    - It filters out specific roles ('ACCOUNTADMIN', 'SYSADMIN', 'SECURITYADMIN') from the grants.
    - For each remaining grant, it generates an ASSIGN PRIVILEGES query based on the privilege, grantee, and grant option.
    - The generated ASSIGN PRIVILEGES queries are returned as a list.
    """
   
    query = "SHOW GRANTS ON ACCOUNT"
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        grants = cursor.fetchall()

        # Generate the ASSIGN PRIVILEGES queries
        queries = []
        for grant in grants:
            privilege = grant[1]
            grantee_name = grant[5]
            grant_option = grant[6]
           

            # Filter out roles 'ACCOUNTADMIN', 'SYSADMIN', and 'SECURITYADMIN'
            if grantee_name not in excluded_roles:
                # Generate the ASSIGN PRIVILEGES query
                assign_query = f"GRANT {privilege} ON ACCOUNT TO ROLE {grantee_name}"
                if grant_option == 'Y':
                    assign_query += " WITH GRANT OPTION"
                assign_query += ";"
                queries.append(assign_query)

        return queries

    finally:
        # Close the cursor
        if cursor:
            cursor.close()


def generate_grant_role_queries(conn):
    # Fetch all roles
    query = "SHOW ROLES"
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        roles = [row[1] for row in cursor.fetchall() if row[1] not in excluded_roles ]

        # Generate the GRANT queries for each role
        queries = []
        for role in roles:
            

            # Fetch the grants of the role
            grants_query = f"SHOW GRANTS OF ROLE {role}"
            cursor.execute(grants_query)
            grants = cursor.fetchall()

            # Generate the GRANT queries for the role's grants
            for grant in grants:
                
                grant_to = grant[2]
                grantee_name = grant[3]
                
                # Filter grants with granted_to = ROLE
                if grant_to == 'ROLE':
                    # Generate the GRANT query for the role
                    grant_query = f"GRANT ROLE {role} TO ROLE {grantee_name};"
                    queries.append(grant_query)

        return queries

    finally:
        # Close the cursor
        if cursor:
            cursor.close()

def generate_create_role_queries(conn):
    """
    Generates GRANT ROLE queries for role grants in Snowflake for creating role heirarchy.
    
    Parameters:
    - conn (Connection): The connection object used to connect to the Snowflake database.
    
    Returns:
    - queries (list): A list of GRANT ROLE queries generated for the role grants.
    
    Notes:
    - This function fetches all roles using the SHOW ROLES statement.
    - It filters out specific roles ('ACCOUNTADMIN', 'SYSADMIN', 'SECURITYADMIN') from the fetched roles.
    - For each remaining role, it fetches the grants of the role using the SHOW GRANTS OF ROLE statement.
    - It generates GRANT ROLE queries based on the grantee and role name for each role's grants.
    - The generated GRANT ROLE queries are returned as a list.
    """
    try:
        cur = conn.cursor()

        # Fetch the list of roles
        cur.execute("SHOW ROLES")
        roles = [row[1] for row in cur.fetchall() if row[1] not in excluded_roles ]

        create_role_queries = []

        # Generate the CREATE ROLE queries for each role
        for role in roles:
            create_role_query = f"CREATE ROLE {role};"
            create_role_queries.append(create_role_query)

        return create_role_queries

    except snowflake.connector.Error as e:
        print(f"An error occurred: {str(e)}")

    finally:
        # Close Snowflake cur
        if cur:
            cur.close()


def generate_create_roles_file(conn, folder_name, file_name, funct):
    
    """
    Generates a file containing the CREATE ROLE queries for roles in Snowflake.

    Parameters:
    - conn (Connection): The connection object used to connect to the Snowflake database.
    - folder_name (str): The name of the folder where the file will be created.
    - file_name (str): The name of the file to be created.
    - funct (function): The function to generate the CREATE ROLE queries.

    Notes:
    - This function generates the CREATE ROLE queries using the provided function (funct).
    - The function (funct) should accept the 'conn' parameter and return a list of CREATE ROLE queries.
    - The folder specified by 'folder_name' will be created if it doesn't already exist.
    - The CREATE ROLE queries will be written to the file specified by 'file_name' within the folder.
    - Each query will be written on a new line in the file.
    - The file path where the queries are written will be printed to the console.
    """
    
    queries = funct(conn)

    # Create the folder if it doesn't exist
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)

    # Write the CREATE ROLE queries to a file
    file_path = os.path.join(folder_name, file_name)
    with open(file_path, 'w') as file:
        for query in queries:
            file.write(query)
            file.write('\n')

    print(f"CREATE ROLE queries have been written to {file_path}")
        
            
@deprecated 
def store_snowflake_object_grant_queries(conn, folder_name, file_name, object_type, object_name, funct):
    
    """
    Stores the Snowflake object grant queries in a file.
    TODO: USE generate_object_grant_queries

    Parameters:
    - conn (Connection): The connection object used to connect to the Snowflake database.
    - folder_name (str): The name of the folder where the file will be created.
    - file_name (str): The name of the file to be created.
    - object_type (str): The type of the Snowflake object (e.g., DATABASE, SCHEMA, TABLE) for which the grant queries will be generated.
    - object_name (str): The name of the specific Snowflake object for which the grant queries will be generated.
    - funct (function): The function to generate the grant queries.

    Notes:
    - This function generates the grant queries for the specified Snowflake object using the provided function (funct).
    - The function (funct) should accept the 'conn', 'object_type', and 'object_name' parameters and return a list of grant queries.
    - The folder specified by 'folder_name' will be created if it doesn't already exist.
    - The grant queries will be written to the file specified by 'file_name' within the folder.
    - Each query will be written on a new line in the file.
    - The file path where the queries are written will be printed to the console.
    """
   
    queries = funct(conn, object_type, object_name)

    # Create the folder if it doesn't exist
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)

    # Write the CREATE ROLE queries to a file
    file_path = os.path.join(folder_name, file_name)
    with open(file_path, 'w') as file:
        for query in queries:
            file.write(query)
            file.write('\n')

    print(f"CREATE ROLE queries have been written to {file_path}")

@deprecated 
def store_snowflake_object_grant_queries(conn, folder_name, file_name, parent_object_type, parent_object_name, object_type, funct):
    """
    Stores the Snowflake object grant queries in a file.
    TODO: USE generate_object_grant_queries


    Parameters:
    - conn (Connection): The connection object used to connect to the Snowflake database.
    - folder_name (str): The name of the folder where the file will be created.
    - file_name (str): The name of the file to be created.
    - parent_object_type (str): The type of the parent Snowflake object (e.g., DATABASE, SCHEMA) that contains the child objects for which the grant queries will be generated.
    - parent_object_name (str): The name of the parent Snowflake object that contains the child objects for which the grant queries will be generated.
    - object_type (str): The type of the child Snowflake objects (e.g., SCHEMA, TABLE) for which the grant queries will be generated.
    - funct (function): The function to generate the grant queries.

    Notes:
    - This function generates the grant queries for the child Snowflake objects under the specified parent object using the provided function (funct).
    - The function (funct) should accept the 'conn', 'parent_object_type', 'parent_object_name', and 'object_type' parameters and return a list of grant queries.
    - The folder specified by 'folder_name' will be created if it doesn't already exist.
    - The grant queries will be written to the file specified by 'file_name' within the folder.
    - Each query will be written on a new line in the file.
    - The file path where the queries are written will be printed to the console.
    """
    
    queries = funct(conn, parent_object_type, parent_object_name, object_type)

    # Create the folder if it doesn't exist
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)

    # Write the CREATE ROLE queries to a file
    file_path = os.path.join(folder_name, file_name)
    with open(file_path, 'w') as file:
        for query in queries:
            file.write(query)
            file.write('\n')

    print(f"CREATE ROLE queries have been written to {file_path}")
                           

###ESTABLISHING SNOWFLAKE SESSION

conn=snowflake.connector.connect(
                user=USER_NAME,
                account=ACCOUNT,
                warehouse=WAREHOUSE,
                database=SNOW_DATABASE,
                schema=SNOW_SCHEMA,
                authenticator='externalbrowser',
                role='ACCOUNTADMIN'
                )
#create cursor
try:
   generate_create_roles_file(conn, "ACCOUNT", "create_roles.sql", generate_create_role_queries) 
   generate_create_roles_file(conn, "ACCOUNT", "create_role_hierarchy.sql", generate_grant_role_queries) 
   generate_create_roles_file(conn, "ACCOUNT", "account_privileges_queries.sql", generate_assign_account_privileges_queries) 
   
   generate_object_grant_queries(conn, "ACCOUNT_MyCompanyDEV", "ACCOUNT", "MyCompanyDEV", "DATABASE", True, False, ['MyCompany'])
   generate_object_grant_queries(conn, "ACCOUNT_MyCompanyDEV", None, None, "INTEGRATION", False, True)
   generate_object_grant_queries(conn, "ACCOUNT_MyCompanyDEV", None, None, "RESOURCE MONITOR", False, True)
   generate_object_grant_queries(conn, "ACCOUNT_MyCompanyDEV", None, None, "WAREHOUSE", False, True)
   generate_object_future_grant_queries(conn, "ACCOUNT_MyCompanyDEV", "ACCOUNT", "MyCompanyDEV", "DATABASE", True, ['MyCompany'])
   
   #@deprecated function calls
   #store_snowflake_object_grant_queries(conn, "ACCOUNT/MyCompany", "database_grant.sql", "DATABASE", "MyCompany", generate_snowflake_object_grant_queries)
   #store_snowflake_object_grant_queries(conn, "ACCOUNT/MyCompany/STAGE_SP", "schema_grant.sql", "SCHEMA", "MyCompany.STAGE_SP", generate_snowflake_object_grant_queries)
   #store_snowflake_object_grant_queries(conn, "ACCOUNT/MyCompany/STAGE_SP", "schema_grant.sql", "SCHEMA", "DATABASE", "MyCompany", generate_snowflake_object_grant_queries)
   
   
   
except Exception as e:
    print(f"An error occurred: {str(e)}")

finally:
    # Close Snowflake connection
    if conn:
        conn.close()
