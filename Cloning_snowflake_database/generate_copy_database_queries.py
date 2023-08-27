import snowflake.connector
import os
####PARAMS


def get_create_schema_queries(connection, database):
    # Create a cursor to execute queries
    cur = connection.cursor()

    # Execute the SHOW SCHEMAS query for the specified database to get the schema information
    cur.execute(f"SHOW SCHEMAS IN DATABASE {database}")
    schema_data = cur.fetchall()

    # Close the cursor
    cur.close()

    create_schema_queries = []
    for row in schema_data:
        name = row[1]
        comment = row[2]
        data_retention_time_in_days = row[4]

        # Generate the CREATE SCHEMA query
        query = f"CREATE SCHEMA {database}.{name}"
        if comment:
            query += f" COMMENT = '{comment}'"
        if data_retention_time_in_days:
            query += f" DATA_RETENTION_TIME_IN_DAYS = {data_retention_time_in_days}"
        query += ";\n"

        create_schema_queries.append(query)

    return create_schema_queries


def get_create_database_queries(connection):
    # Create a cursor to execute queries
    cur = connection.cursor()

    # Execute the SHOW DATABASES query to get the database information
    cur.execute("SHOW DATABASES")
    database_data = cur.fetchall()

    # Close the cursor
    cur.close()

    create_database_queries = []
    for row in database_data:
        name = row[1]
        owner = row[5]
        comment = row[6]
        data_retention_time_in_days = row[8]
        kind = row[9]

        if kind == 'STANDARD':
            # Generate the CREATE DATABASE query
            query = f"CREATE DATABASE {name}"
            if comment:
                query += f" COMMENT = '{comment}'"
            if data_retention_time_in_days:
                query += f" DATA_RETENTION_TIME_IN_DAYS = {data_retention_time_in_days}"
            query += ";\n"

            create_database_queries.append(query)

    return create_database_queries



def write_queries_to_file(queries, filename):
    with open(filename, 'w') as file:
        for query in queries:
            file.write(f"{query}\n")

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
    
    write_queries_to_file(get_create_database_queries(conn), "create_databases.sql")

    write_queries_to_file(get_create_schema_queries(conn, 'turvo'), "create_schemas.sql")

   
   
except Exception as e:
    print(f"An error occurred: {str(e)}")

finally:
    # Close Snowflake connection
    if conn:
        conn.close()
