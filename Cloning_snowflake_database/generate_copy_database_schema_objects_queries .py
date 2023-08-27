import snowflake.connector
import os
####PARAMS



def generate_procedure_queries(connection, schema_name):
    cursor = connection.cursor()
    cursor.execute(f"SHOW PROCEDURES IN SCHEMA {schema_name}")
    procedures_data = cursor.fetchall()
    cursor.close()

    create_procedure_queries = []

    for row in procedures_data:
        if row[3] == 'Y':
            continue
        procedure_name_with_signature = row[8]  # Assuming the procedure name with signature is in the second column
        procedure_name = procedure_name_with_signature.split(' RETURN')[0]  # Extracting procedure name
        ddl_query = f"SELECT GET_DDL('procedure', '{schema_name}.{procedure_name}')"
        cursor = connection.cursor()
        cursor.execute(ddl_query)
        create_procedure_query = cursor.fetchone()[0]
        cursor.close()
        create_procedure_queries.append(create_procedure_query)

    return create_procedure_queries

def generate_function_queries(connection, schema_name):
    cursor = connection.cursor()
    cursor.execute(f"SHOW FUNCTIONS IN SCHEMA {schema_name}")
    functions_data = cursor.fetchall()
    cursor.close()

    create_function_queries = []

    for row in functions_data:
        if row[3] == 'Y':
            continue
        function_name_with_signature = row[8]  # Assuming the function name with signature is in the second column
        function_name = function_name_with_signature.split(' RETURN')[0]  # Extracting function name
        ddl_query = f"SELECT GET_DDL('function', '{schema_name}.{function_name}')"
        cursor = connection.cursor()
        cursor.execute(ddl_query)
        create_function_query = cursor.fetchone()[0]
        cursor.close()
        create_function_queries.append(create_function_query)

    return create_function_queries

def generate_view_queries(connection, schema_name):
    cursor = connection.cursor()
    cursor.execute(f"SHOW VIEWS IN SCHEMA {schema_name}")
    views_data = cursor.fetchall()
    cursor.close()

    create_view_queries = []

    for row in views_data:
        view_name = row[1]  # Assuming the view name is in the second column
        ddl_query = f"SELECT GET_DDL('view', '{schema_name}.{view_name}')"
        cursor = connection.cursor()
        cursor.execute(ddl_query)
        create_view_query = cursor.fetchone()[0]
        cursor.close()
        create_view_queries.append(create_view_query)

    return create_view_queries

def generate_create_stream_queries(connection, schema_name):
    cursor = connection.cursor()
    cursor.execute(f"SHOW STREAMS IN SCHEMA {schema_name}")
    streams_data = cursor.fetchall()
    cursor.close()

    create_stream_queries = []

    for row in streams_data:
        stream_name = row[1]  # Assuming the stream name is in the second column
        ddl_query = f"SELECT GET_DDL('stream', '{stream_name}')"
        cursor = connection.cursor()
        cursor.execute(ddl_query)
        create_stream_query = cursor.fetchone()[0]
        cursor.close()
        create_stream_queries.append(create_stream_query)
    print(create_stream_queries)
    return create_stream_queries

def generate_file_format_queries(connection, schema_name):
    cursor = connection.cursor()
    cursor.execute(f"SHOW FILE FORMATS IN SCHEMA {schema_name}")
    file_formats_data = cursor.fetchall()
    cursor.close()

    create_file_format_queries = []

    for row in file_formats_data:
        file_format_name = row[1]  # Assuming the file format name is in the first column
        ddl_query = f"SELECT GET_DDL('file_format', '{schema_name}.{file_format_name}')"
        cursor = connection.cursor()
        cursor.execute(ddl_query)
        create_file_format_query = cursor.fetchone()[0]
        cursor.close()
        create_file_format_queries.append(create_file_format_query)

    return create_file_format_queries

def generate_pipe_queries(connection, schema_name):
    cursor = connection.cursor()
    cursor.execute(f"SHOW PIPES IN SCHEMA {schema_name}")
    pipes_data = cursor.fetchall()
    cursor.close()

    create_pipe_queries = []

    for row in pipes_data:
        pipe_name = row[1]  # Assuming the pipe name is in the first column
        ddl_query = f"SELECT GET_DDL('pipe', '{schema_name}.{pipe_name}')"
        cursor = connection.cursor()
        cursor.execute(ddl_query)
        create_pipe_query = cursor.fetchone()[0]
        cursor.close()
        create_pipe_queries.append(create_pipe_query)

    return create_pipe_queries

def generate_table_queries(connection, schema_name):
    cursor = connection.cursor()
    cursor.execute(f"SHOW TABLES IN SCHEMA {schema_name}")
    tables_data = cursor.fetchall()
    cursor.close()

    create_table_queries = []

    for row in tables_data:
        table_name = row[1]  # Assuming the table name is in the second column
        ddl_query = f"SELECT GET_DDL('table', '{schema_name}.{table_name}')"
        cursor = connection.cursor()
        cursor.execute(ddl_query)
        create_table_query = cursor.fetchone()[0]
        cursor.close()
        create_table_queries.append(create_table_query)

    return create_table_queries

def generate_tasks_queries(connection, schema_name):
    cursor = connection.cursor()
    cursor.execute(f"SHOW TASKS IN SCHEMA {schema_name}")
    
    # This query will sort tasks in heirchary putting parent tasks first
    cursor.execute ("""WITH RECURSIVE tasks_ AS (
        select
            "database_name" || '.' || "schema_name" || '.' || "name" as task_name,
            "state" as state,
            "predecessors"::varchar as parent_value,
            1 as level 
        ,
            uuid_string() as h_id
        from
            table(result_scan(last_query_id()))
        where
            parent_value = PARSE_JSON('[]')
        UNION ALL
        select
            tab.task_name,
            tab.state, 
            tab.parent_value,
            tasks_.level + 1 as level,
            h_id
        from
            (
            select
                "database_name" || '.' || "schema_name" || '.' || "name" as task_name,
                "state" as state,
                "predecessors" as predecessors,
                pred.value::varchar as parent_value
            from
                table(result_scan(last_query_id())) ,
                lateral flatten (predecessors) pred) tab
        join tasks_ on
            tab.parent_value = tasks_.task_name 
        ) 

        select
            TASK_NAME,
            STATE,
            PARENT_VALUE
        from
            tasks_
        order by
            H_ID,
            level asc ;
        """)
    tasks_data = cursor.fetchall()
    cursor.close()

    create_task_queries = []

    for row in tasks_data:
        task_name = row[0]
        print(row[0], ' -- ', row[1],' -- ', row[2])
        ddl_query = f"SELECT GET_DDL('task', '{task_name}')"
        cursor = connection.cursor()
        cursor.execute(ddl_query)
        create_task_query = cursor.fetchone()[0]
        cursor.close()
        create_task_queries.append(create_task_query)
        create_task_queries.append('\n')

    create_task_queries.append('\n #######################   START TASKS #####################################\n')
    tasks_data ## Reverse list as child should be started before parent.
    for row in tasks_data:
       # print(row[0], ' -- ', row[1],' -- ', row[2])
        if row[1] != 'started':
            continue
        create_task_queries.append(f'Alter task {row[0]} resume')

    return create_task_queries



def generate_alert_queries(connection, schema_name):
    cursor = connection.cursor()
    cursor.execute(f"SHOW ALERTS IN SCHEMA {schema_name}")
    alerts_data = cursor.fetchall()
    cursor.close()

    alert_queries = []

    for row in alerts_data:
        alert_name = row[1]  # Assuming the alert name is in the second column
        ddl_query = f"SELECT GET_DDL('alert', '{schema_name}.{alert_name}')"
        cursor = connection.cursor()
        cursor.execute(ddl_query)
        alert_query = cursor.fetchone()[0]
        cursor.close()
        alert_queries.append(alert_query)

    return alert_queries

def generate_sequence_queries(connection, schema_name):
    cursor = connection.cursor()
    cursor.execute(f"SHOW SEQUENCES IN SCHEMA {schema_name}")
    sequences_data = cursor.fetchall()
    cursor.close()

    sequence_queries = []

    for row in sequences_data:
        sequence_name = row[1]  # Assuming the sequence name is in the second column
        ddl_query = f"SELECT GET_DDL('sequence', '{schema_name}.{sequence_name}')"
        cursor = connection.cursor()
        cursor.execute(ddl_query)
        sequence_query = cursor.fetchone()[0]
        cursor.close()
        sequence_queries.append(sequence_query)

    return sequence_queries

def generate_masking_policy_queries(connection, schema_name):
    cursor = connection.cursor()
    cursor.execute(f"SHOW MASKING POLICIES IN SCHEMA {schema_name}")
    masking_policies_data = cursor.fetchall()
    cursor.close()

    masking_policy_queries = []

    for row in masking_policies_data:
        masking_policy_name = row[1]  # Assuming the masking policy name is in the second column
        ddl_query = f"SELECT GET_DDL('policy', '{schema_name}.{masking_policy_name}')"
        cursor = connection.cursor()
        cursor.execute(ddl_query)
        masking_policy_query = cursor.fetchone()[0]
        cursor.close()
        masking_policy_queries.append(masking_policy_query)

    return masking_policy_queries

def generate_row_access_policy_queries(connection, schema_name):
    cursor = connection.cursor()
    cursor.execute(f"SHOW ROW ACCESS POLICIES IN SCHEMA {schema_name}")
    row_access_policies_data = cursor.fetchall()
    cursor.close()

    row_access_policy_queries = []

    for row in row_access_policies_data:
        row_access_policy_name = row[1]  # Assuming the row access policy name is in the second column
        ddl_query = f"SELECT GET_DDL('policy', '{schema_name}.{row_access_policy_name}')"
        cursor = connection.cursor()
        cursor.execute(ddl_query)
        row_access_policy_query = cursor.fetchone()[0]
        cursor.close()
        row_access_policy_queries.append(row_access_policy_query)

    return row_access_policy_queries


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
   
    write_queries_to_file(generate_tasks_queries(conn, 'tur.stage'), "create_tasks.sql")

except Exception as e:
    print(f"An error occurred: {str(e)}")

finally:
    # Close Snowflake connection
    if conn:
        conn.close()
