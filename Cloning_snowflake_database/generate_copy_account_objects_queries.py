import snowflake.connector
import os
####PARAMS



def generate_create_resource_monitor_queries(connection):
    # Create a cursor to execute queries
    cur = connection.cursor()

    # Execute the SHOW RESOURCE MONITORS query to get the resource monitor information
    cur.execute("SHOW RESOURCE MONITORS")
    resource_monitors_data = cur.fetchall()

    # Close the cursor
    cur.close()


    create_resource_monitor_queries = []
    for row in resource_monitors_data:
        name = row[0]
        credit_quota = row[1]
        notify_users = row[14]
        notify_at = row[8]
        suspend_at = row[9]
        suspend_immediately_at = row[10]
        frequency = row[5]

        notify_triggers = ", ".join([f"ON {val} PERCENT DO NOTIFY" for val in notify_at.split(",")])
        suspend_triggers = ", ".join([f"ON {val} PERCENT DO SUSPEND" for val in suspend_at.split(",")])
        suspend_immediately_triggers = ", ".join([f"ON {val} PERCENT DO SUSPEND_IMMEDIATE" for val in suspend_immediately_at.split(",")])

        # Generate the CREATE RESOURCE MONITOR query
        query = f"CREATE OR REPLACE RESOURCE MONITOR {name} WITH CREDIT_QUOTA = {credit_quota}\n" \
                f"  NOTIFY_USERS = ({notify_users})\n" \
                f"  FREQUENCY = {frequency}\n" \
                f"  TRIGGERS {notify_triggers}, {suspend_triggers}, {suspend_immediately_triggers}\n"

        query += ";\n"
        create_resource_monitor_queries.append(query)

    return create_resource_monitor_queries


def get_create_warehouse_queries(connection):
    # Create a cursor to execute queries
    cur = connection.cursor()

    # Execute the SHOW WAREHOUSES query to get the warehouse information
    cur.execute("SHOW WAREHOUSES")
    warehouses_data = cur.fetchall()

    # Generate CREATE WAREHOUSE queries based on the data
    create_warehouse_queries = []
    for row in warehouses_data:
        name = row[0]
        w_type = row[2]
        size = row[3]
        max_cluster_count = row[5]
        min_cluster_count = row[4]
        scaling_policy = row[30]
        auto_suspend = row[11]
        auto_resume = row[12]
        initially_suspended = True
        resource_monitor = row[24]
        comment = row[21]
        enable_query_acceleration = row[22]
        query_acceleration_max_scale_factor = row[23]

        query = f"CREATE WAREHOUSE IF NOT EXISTS {name} " \
                f"WAREHOUSE_TYPE = '{w_type}' " \
                f"WAREHOUSE_SIZE = '{size}' " \
                f"MAX_CLUSTER_COUNT = {max_cluster_count} " \
                f"MIN_CLUSTER_COUNT = {min_cluster_count} " \
                f"SCALING_POLICY = '{scaling_policy}' "

        if auto_suspend is not None:
            query += f"AUTO_SUSPEND = {auto_suspend} "
        if auto_resume is not None:
            query += f"AUTO_RESUME = {auto_resume} "
        if initially_suspended is not None:
            query += f"INITIALLY_SUSPENDED = {initially_suspended} "
        
        if resource_monitor is not None and resource_monitor != 'null':
            query += f"RESOURCE_MONITOR = '{resource_monitor}' "
        if comment is not None:
            query += f"COMMENT = '{comment}' "
        if enable_query_acceleration is not None :
            query += f"ENABLE_QUERY_ACCELERATION = {enable_query_acceleration} "
        if query_acceleration_max_scale_factor is not None:
            query += f"QUERY_ACCELERATION_MAX_SCALE_FACTOR = {query_acceleration_max_scale_factor} "

        query += ";"

        create_warehouse_queries.append(query)

    # Close the cursor, but keep the connection open for external handling
    cur.close()

    return create_warehouse_queries

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
    
    write_queries_to_file(get_create_warehouse_queries(conn), "create_warehouses.sql")
    # Call the combined function to create and write the queries
    write_queries_to_file(generate_create_resource_monitor_queries(conn), "create_resource_monitors.sql")

   
except Exception as e:
    print(f"An error occurred: {str(e)}")

finally:
    # Close Snowflake connection
    if conn:
        conn.close()
