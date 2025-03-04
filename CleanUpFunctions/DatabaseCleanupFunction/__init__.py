import logging
import azure.functions as func
import pyodbc
from azure.identity import DefaultAzureCredential
import os
from datetime import datetime, timedelta

def main(mytimer: func.TimerRequest) -> None:
    logging.info('Database cleanup function triggered')

    try:
        # Log environment variables (mask sensitive data)
        server = os.environ.get("SQL_SERVER")
        database = os.environ.get("SQL_DATABASE")
        retention_days = os.environ.get("RETENTION_DAYS", "90")

        logging.info(f"SQL_SERVER configured: {'Yes' if server else 'No'}")
        logging.info(f"SQL_DATABASE configured: {'Yes' if database else 'No'}")
        logging.info(f"RETENTION_DAYS configured: {retention_days}")

        if not server or not database:
            raise ValueError("Missing required environment variables: SQL_SERVER and/or SQL_DATABASE")

        # Log available ODBC drivers
        logging.info("Available ODBC drivers:")
        logging.info(pyodbc.drivers())

        # Get access token using managed identity
        credential = DefaultAzureCredential()
        token = credential.get_token("https://database.windows.net/.default")
        logging.info("Successfully acquired token")

        # Connection string with Authentication
        conn_str = (
            f"Driver={{ODBC Driver 17 for SQL Server}};"
            f"Server=tcp:{server},1433;"
            f"Database={database};"
            "Authentication=ActiveDirectoryMSI;"  # Add this line
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=30;"
        )
        
        logging.info("Attempting to connect to database...")
        logging.info(f"Connection string (masked): Driver={{ODBC Driver 17 for SQL Server}};Server=tcp:{server},1433;Database={database}")

        try:
            # Try connecting without token first
            logging.info("Attempting connection with MSI authentication...")
            conn = pyodbc.connect(conn_str)
            logging.info("Successfully connected to database using MSI authentication")
        except pyodbc.Error as msi_error:
            logging.warning(f"MSI authentication failed: {str(msi_error)}")
            logging.info("Attempting connection with access token...")
            try:
                # Fallback to token-based connection
                conn_str_token = (
                    f"Driver={{ODBC Driver 17 for SQL Server}};"
                    f"Server=tcp:{server},1433;"
                    f"Database={database};"
                    "Encrypt=yes;"
                    "TrustServerCertificate=no;"
                    "Connection Timeout=30;"
                )
                conn = pyodbc.connect(conn_str_token, attrs_before={1256: token.token})
                logging.info("Successfully connected to database using access token")
            except pyodbc.Error as token_error:
                logging.error(f"Token authentication failed: {str(token_error)}")
                raise

        with conn:
            # Test the connection with a simple query
            with conn.cursor() as cursor:
                try:
                    cursor.execute("SELECT @@VERSION")
                    version = cursor.fetchone()[0]
                    logging.info(f"Connected to SQL Server version: {version}")
                except Exception as test_error:
                    logging.error(f"Error testing connection: {str(test_error)}")
                    raise

                cleanup_queries = [
                    """
                    DELETE FROM Logs 
                    WHERE CreatedDate < DATEADD(day, ?, GETDATE())
                    """,
                    """
                    DELETE FROM AuditTrail 
                    WHERE Timestamp < DATEADD(day, ?, GETDATE())
                    """
                ]

                total_deleted = 0
                for query in cleanup_queries:
                    try:
                        # First check if the table exists
                        table_name = query.split("FROM")[1].split("WHERE")[0].strip()
                        cursor.execute(f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'")
                        if cursor.fetchone()[0] == 0:
                            logging.warning(f"Table {table_name} does not exist, skipping...")
                            continue

                        # Count records to be deleted
                        count_query = query.replace("DELETE FROM", "SELECT COUNT(*) FROM")
                        cursor.execute(count_query, -int(retention_days))
                        to_delete_count = cursor.fetchone()[0]
                        logging.info(f"Found {to_delete_count} records to delete from {table_name}")

                        # Execute the delete
                        cursor.execute(query, -int(retention_days))
                        deleted_rows = cursor.rowcount
                        total_deleted += deleted_rows
                        logging.info(f"Deleted {deleted_rows} rows from {table_name}")
                        conn.commit()
                    except Exception as query_error:
                        logging.error(f"Error executing query on {table_name}: {str(query_error)}")
                        conn.rollback()

                logging.info(f"Database cleanup completed. Total rows deleted: {total_deleted}")

    except ValueError as ve:
        logging.error(f"Configuration error: {str(ve)}")
        raise
    except Exception as e:
        logging.error(f"Error in database cleanup function: {str(e)}")
        logging.error(f"Error type: {type(e)}")
        logging.error(f"Error args: {e.args if hasattr(e, 'args') else 'No args available'}")
        raise