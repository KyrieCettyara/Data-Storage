import luigi
from datetime import datetime
import logging
import time
import pandas as pd
from source_data.pipeline.utils.db_conn import db_connection
from source_data.pipeline.utils.read_sql import read_sql_file
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


# Define DIR
DIR_ROOT_PROJECT = os.getenv("DIR_ROOT_PROJECT")
DIR_TEMP_LOG = os.getenv("DIR_TEMP_LOG")
DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")
DIR_EXTRACT_QUERY = os.getenv("DIR_EXTRACT_QUERY")
DIR_LOG = os.getenv("DIR_LOG")
SENTRY_DSN = os.getenv("SENTRY_DSN")

class GlobalParams(luigi.Config):
    CurrentTimestampParams = luigi.DateSecondParameter(default=datetime.now())


class Extract(luigi.Task):

    current_timestamp = GlobalParams().CurrentTimestampParams
    
    # Define tables to be extracted from db sources
    tables_to_extract = ['address',
                        'address_status',
                        'author',
                        'book_author',
                        'book_language',
                        'book',
                        'country',
                        'cust_order',
                        'customer_address',
                        'customer',
                        'order_history',
                        'order_line',
                        'order_status',
                        'publisher',
                        'shipping_method']
    
    def requires(self):
        pass


    def run(self):        
        try:
            # Configure logging
            logging.basicConfig(filename = r'D:\Kyrie\Pacmann\DataStorageManagement\Project\pacmann-bookstore\source_data\pipeline\temp\log\logs.log', 
                                level = logging.INFO, 
                                format = '%(asctime)s - %(levelname)s - %(message)s')
            
            # Define db connection engine
            src_engine, _ = db_connection()
            
            # Define the query using the SQL content
            extract_query = read_sql_file(
                file_path = r'D:\Kyrie\Pacmann\DataStorageManagement\Project\pacmann-bookstore\source_data\pipeline\src_query\extract\all-tables.sql'
            )
            
            start_time = time.time()  # Record start time
            logging.info("==================================STARTING EXTRACT DATA=======================================")
            
            for index, table_name in enumerate(self.tables_to_extract):
                try:
                    # Read data into DataFrame
                    df = pd.read_sql_query(extract_query.format(table_name = table_name), src_engine)

                    # Write DataFrame to CSV
                    df.to_csv(r'D:\Kyrie\Pacmann\DataStorageManagement\Project\pacmann-bookstore\source_data\pipeline\temp\data' + f"\{table_name}.csv", index=False)
                    
                    logging.info(f"EXTRACT '{table_name}' - SUCCESS.")
                    
                except Exception:
                    logging.error(f"EXTRACT '{table_name}' - FAILED.")  
                    raise Exception(f"Failed to extract '{table_name}' tables")
            
            logging.info(f"Extract All Tables From Sources - SUCCESS")
            
            end_time = time.time()  # Record end time
            execution_time = end_time - start_time  # Calculate execution time
            
            # Get summary
            summary_data = {
                'timestamp': [datetime.now()],
                'task': ['Extract'],
                'status' : ['Success'],
                'execution_time': [execution_time]
            }
            
            # Get summary dataframes
            summary = pd.DataFrame(summary_data)
            
            # Write DataFrame to CSV
            ##summary.to_csv(r'D:\Kyrie\Pacmann\DataStorageManagement\Project\pacmann-bookstore\source_data\pipeline\temp\data\extract-summary.csv', index = False)
            summary.to_csv(self.output().path, index=False)
                    
        except Exception:   
            logging.info(f"Extract All Tables From Sources - FAILED")
             
            # Get summary
            summary_data = {
                'timestamp': [datetime.now()],
                'task': ['Extract'],
                'status' : ['Failed'],
                'execution_time': [0]
            }
            
            # Get summary dataframes
            summary = pd.DataFrame(summary_data)
            
            # Write DataFrame to CSV
            summary.to_csv(self.output().path, index=False)
            
            # Write exception
            raise Exception(f"FAILED to execute EXTRACT TASK !!!")
        
        logging.info("==================================ENDING EXTRACT DATA=======================================")
                
    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget(r'D:\Kyrie\Pacmann\DataStorageManagement\Project\pacmann-bookstore\source_data\pipeline\temp\data\extracted_data.csv')

    

