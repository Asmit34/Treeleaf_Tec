from sqlalchemy import create_engine
import pandas as pd

class DatabaseManager:
    def __init__(self, db_config):
        # Create the connection string using db_config
        self.db_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        
    def save_to_db(self, table_name, data_frame):
        try:
            # Create SQLAlchemy engine
            engine = create_engine(self.db_url)
            
            # Use pandas to save the data to the database
            data_frame.to_sql(table_name, engine, if_exists='replace', index=False, method='multi')
            print(f"Data successfully saved to {table_name} table.")
            
        except Exception as e:
            print(f"An error occurred while saving data to the database: {e}")
