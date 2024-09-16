import mysql.connector
from mysql.connector import Error

def create_connection(host_name, user_name, user_password, db_name):
    """Create a connection to the MySQL database."""
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print(f"Connection to {db_name} successful")
    except Error as e:
        print(f"The error '{e}' occurred")
    return connection

def fetch_data_from_source(source_connection, table_name):
    """Fetch data from the source table."""
    cursor = source_connection.cursor(dictionary=True)
    query = f"SELECT * FROM {table_name}"
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    return result

def get_table_schema(source_connection, table_name):
    """Get the schema of the source table."""
    cursor = source_connection.cursor()
    cursor.execute(f"SHOW CREATE TABLE {table_name}")
    result = cursor.fetchone()
    cursor.close()
    return result[1]  

def create_table_in_target(target_connection, create_table_query):
    """Create a table in the target database using the source schema."""
    cursor = target_connection.cursor()
    try:
        cursor.execute(create_table_query)
        target_connection.commit()
        print("Table created successfully")
    except Error as e:
        print(f"The error '{e}' occurred during table creation")
    finally:
        cursor.close()

def check_table_exists(target_connection, table_name):
    """Check if the table exists in the target database."""
    cursor = target_connection.cursor()
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    result = cursor.fetchone()
    cursor.close()
    return result is not None

def insert_data_into_target(target_connection, table_name, data):
    """Insert data into the target table."""
    cursor = target_connection.cursor()

    columns = ', '.join(data[0].keys())
    values_placeholder = ', '.join(['%s'] * len(data[0]))
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values_placeholder})"

    values = [tuple(item.values()) for item in data]

    try:
        cursor.executemany(insert_query, values)
        target_connection.commit()
        print(f"{cursor.rowcount} records inserted successfully into {table_name} table")
    except Error as e:
        print(f"The error '{e}' occurred during data insertion")
    finally:
        cursor.close()

def migrate_data(source_connection, target_connection, table_names):
    """Migrate data from the source to the target database."""
    for table_name in table_names:
        print(f"Migrating data from table: {table_name}")
        
        
        if not check_table_exists(target_connection, table_name):
            print(f"Table {table_name} does not exist in target DB, creating it...")
            # Get the source table schema and create it in the target DB
            create_table_query = get_table_schema(source_connection, table_name)
            create_table_in_target(target_connection, create_table_query)
        
        
        data = fetch_data_from_source(source_connection, table_name)
        if data:
            insert_data_into_target(target_connection, table_name, data)
        else:
            print(f"No data available in the {table_name} table")

def main():
    # alter this for your source and target databases
    source_db = {
        "host": "source_host",
        "user": "source_user",
        "passwd": "source_passwd",
        "database": "source_db"
    }

    target_db = {
        "host": "target_host",
        "user": "target_user",
        "passwd": "target_passwd",
        "database": "target_db"
    }

    
    table_names = ["table1", "table2", "table3"]  # Specify the tables you want to migrate

   
    source_connection = create_connection(source_db["host"], source_db["user"], source_db["passwd"], source_db["database"])
    target_connection = create_connection(target_db["host"], target_db["user"], target_db["passwd"], target_db["database"])

    if source_connection and target_connection:
        migrate_data(source_connection, target_connection, table_names)

    
    if source_connection:
        source_connection.close()
    if target_connection:
        target_connection.close()

    if __name__ == "__main__":
     main()
