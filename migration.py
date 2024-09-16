import mysql.connector
from mysql.connector import Error

def create_connection(host_name, user_name, user_password, db_name):
    """Create a connection to the MySQL database specified by the parameters."""

    connection = None

    try:
        connection = mysql.connector.conncect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("Connection to Mysql DB: {db_name} successful")
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
    
    def insert_data_into_target(target_connection, table_name, data):
        """Insert data into the target table."""
        cursor = target.connection.cursor()

        # assuming all columns are to be copied: this can be customized
        columns = ','.join(data[0].keys())
        values_placeholder = ",".join(["%s"] * len(data[0]))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values_placeholder})"

        values = [tuple(item.values()) for item in data]

        try:
            cursor.executemany(insert_query, values)
            target_connection.commit()
            print(f"{cursor.rowcount} inserted successfully into {table_name} table")
        except Error as e:
            print(f"The error '{e}' occurred")
        finally:
            cursor.close()

            def migrate_data(source_connection, target_connection, table_name):
                """Migrate data from the source table to the target table."""
                data = fetch_data_from_source(source_connection, table_name)
                if data:
                    insert_data_into_target(target_connection, table_name, data)
                else:
                    print(f"No data available in the {table_name} table")

                    def main():
                        # source and target database details
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

                        table_name = "detector_lane_map"

                        # create connections to the source and target databases
                        source_connection = create_connection(source_db["host"], source_db["user"], source_db["passwd"], source_db["database"])
                        target_connection = create_connection(target_db["host"], target_db["user"], target_db["passwd"], target_db["database"])

                        if source_connection and target_connection:
                            migrate_data(source_connection, target_connection, table_name)

                            # close the connections
                            if source_connection:
                                source_connection.close()
                            if target_connection:
                                target_connection.close()

                                if __name__ == "__main__":
                                    main()






