import json
from typing import List, Dict, Any, Optional
from datetime import datetime

# Note: Install with: pip install snowflake-connector-python
# import snowflake.connector


class SnowflakeConnector:
    """Manages Snowflake database connections and operations"""
    
    def __init__(self, account: str, user: str, password: str, 
                 database: str = "RAW_DB_DEV", schema: str = "FIELDROUTES",
                 warehouse: str = "ALTAPESTANALYTICS"):
        self.account = account
        self.user = user
        self.password = password
        self.database = database
        self.schema = schema
        self.warehouse = warehouse
        self.connection = None
        self.cursor = None
    
    def connect(self):
        """Establish connection to Snowflake"""
        try:
            import snowflake.connector
            
            self.connection = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                database=self.database,
                schema=self.schema,
                warehouse=self.warehouse
            )
            self.cursor = self.connection.cursor()
            print(f"Connected to Snowflake: {self.database}.{self.schema}")
            return True
        except Exception as e:
            print(f"Failed to connect to Snowflake: {e}")
            return False
    
    def disconnect(self):
        """Close Snowflake connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Disconnected from Snowflake")
    
    def create_schema_if_not_exists(self):
        """Create schema if it doesn't exist"""
        try:
            self.cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.database}.{self.schema}")
            print(f"Schema {self.database}.{self.schema} is ready")
            return True
        except Exception as e:
            print(f"Error creating schema: {e}")
            return False
    
    def create_raw_table_if_not_exists(self, table_name: str):
        """Create a raw JSON table if it doesn't exist"""
        # First ensure schema exists
        if not self.create_schema_if_not_exists():
            return False
            
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.database}.{self.schema}.{table_name} (
            ID NUMBER AUTOINCREMENT PRIMARY KEY,
            RAW_JSON VARIANT,
            LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            SOURCE_FILE VARCHAR(255),
            BATCH_ID VARCHAR(255)
        )
        """
        
        try:
            self.cursor.execute(create_table_sql)
            print(f"Table {table_name} is ready")
            return True
        except Exception as e:
            print(f"Error creating table: {e}")
            return False
    
    def insert_raw_json_batch(self, table_name: str, json_records: List[Dict[str, Any]], 
                             batch_id: str = None, source_file: str = None):
        """Insert a batch of JSON records into the raw table"""
        if not json_records:
            print("No records to insert")
            return 0
        
        if batch_id is None:
            batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Prepare data for bulk insert
        data_to_insert = []
        for record in json_records:
            data_to_insert.append((
                json.dumps(record),  # RAW_JSON
                source_file,         # SOURCE_FILE
                batch_id            # BATCH_ID
            ))
        
        insert_sql = f"""
        INSERT INTO {self.database}.{self.schema}.{table_name} 
        (RAW_JSON, SOURCE_FILE, BATCH_ID)
        SELECT PARSE_JSON(column1), column2, column3
        FROM VALUES %s
        """
        
        try:
            # Use parameterized query for safe JSON insertion
            insert_sql_param = f"""
            INSERT INTO {self.database}.{self.schema}.{table_name} 
            (RAW_JSON, SOURCE_FILE, BATCH_ID)
            VALUES (PARSE_JSON(%s), %s, %s)
            """
            
            # Prepare data for executemany with proper JSON formatting
            param_data = []
            for record in json_records:
                try:
                    # Use compact JSON serialization with ASCII escaping for Snowflake compatibility
                    json_str = json.dumps(record, ensure_ascii=True, separators=(',', ':'))
                    param_data.append((
                        json_str,        # JSON string
                        source_file,     # SOURCE_FILE
                        batch_id        # BATCH_ID
                    ))
                except (TypeError, ValueError) as e:
                    print(f"Skipping invalid JSON record: {e}")
                    continue
            
            # Execute bulk insert using parameterized queries
            self.cursor.executemany(insert_sql_param, param_data)
            inserted_count = len(param_data)
            print(f"Inserted {inserted_count} records into {table_name}")
            return inserted_count
        except Exception as e:
            print(f"Error inserting records: {e}")
            self.connection.rollback()
            return 0
    
    def commit(self):
        """Commit the current transaction"""
        if self.connection:
            self.connection.commit()
            print("Transaction committed")


class SnowflakePestRoutesLoader:
    """Handles loading PestRoutes data into Snowflake"""
    
    def __init__(self, snowflake_conn: SnowflakeConnector):
        self.snowflake = snowflake_conn
    
    def load_disbursement_items(self, items: List[Dict[str, Any]], 
                               date_range: Dict[str, str] = None) -> bool:
        """Load disbursement items into Snowflake RAW table"""
        table_name = "DISBURSEMENTITEM_FACT"
        
        # Ensure table exists
        if not self.snowflake.create_raw_table_if_not_exists(table_name):
            return False
        
        # Generate batch ID with date range info
        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        if date_range:
            batch_id += f"_{date_range['start']}_{date_range['end']}"
        
        # Insert in chunks to avoid memory issues
        chunk_size = 5000
        total_inserted = 0
        
        for i in range(0, len(items), chunk_size):
            chunk = items[i:i + chunk_size]
            inserted = self.snowflake.insert_raw_json_batch(
                table_name, 
                chunk,
                batch_id=batch_id,
                source_file="pestroutes_api"
            )
            total_inserted += inserted
            
            # Commit after each chunk
            self.snowflake.commit()
            print(f"Progress: {total_inserted}/{len(items)} records loaded")
        
        print(f"Successfully loaded {total_inserted} disbursement items to Snowflake")
        return total_inserted == len(items)


# Example usage
if __name__ == "__main__":
    # Snowflake credentials
    SNOWFLAKE_ACCOUNT = "AUEDKKB-TH88792.snowflakecomputing.com"
    SNOWFLAKE_USER = "EVANUNICK"
    SNOWFLAKE_PASSWORD = "YOUR_PASSWORD_HERE"  # Replace with actual password
    
    # Sample data
    sample_items = [
        {
            "gatewayDisbursementEntryID": "5944696",
            "gatewayDisbursementID": "5093",
            "dateCreated": "2025-06-05 22:06:50",
            "amount": "100.00"
        },
        {
            "gatewayDisbursementEntryID": "5944697",
            "gatewayDisbursementID": "5093",
            "dateCreated": "2025-06-05 22:06:51",
            "amount": "200.00"
        }
    ]
    
    # Connect and load
    snowflake_conn = SnowflakeConnector(
        account=SNOWFLAKE_ACCOUNT.replace(".snowflakecomputing.com", ""),
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD
    )
    
    if snowflake_conn.connect():
        loader = SnowflakePestRoutesLoader(snowflake_conn)
        loader.load_disbursement_items(sample_items, {"start": "2025-06-05", "end": "2025-06-06"})
        snowflake_conn.disconnect()