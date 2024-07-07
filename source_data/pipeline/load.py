import time
import luigi
import datetime
import traceback
import pandas as pd
from sqlalchemy import text

from .extract import Extract
from source_data.pipeline.utils.db_conn import db_connection
from .utils.log_config import copy

class GlobalParams(luigi.Config):
    CurrentTimestampParams = luigi.DateSecondParameter(default=datetime.datetime.now())

class Load(luigi.Task):
    
    current_timestamp = GlobalParams().CurrentTimestampParams

    tables = ['address',
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
        return Extract()

    def run(self):
        logger = copy("load", self.current_timestamp)
        logger.info("==================================TRUNCATE DATA=======================================")

        _, target_engine = db_connection()

        try:
            with target_engine.connect() as conn:
                for index, table_name in enumerate(self.tables):
                ##for table in tables:
                    select_query = text(f"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'")
                    result = conn.execute(select_query)

                    if result.scalar_one_or_none():
                        truncate_query = text(f"TRUNCATE public.{table_name} CASCADE")
                        conn.execute(truncate_query)
                        conn.commit()
                        logger.info(f"TRUNCATE {table_name} - SUCCESS")
                    else:
                        logger.info(f"Table '{table_name}' does not exist, skipping truncate operation")
            logger.info("TRUNCATE ALL TABLES - DONE")

        except Exception as e:
            logger.error(f"TRUNCATE DATA - FAILED: {e}\n{traceback.format_exc()}")
        
        logger.info("==================================END OF PREPARATION=======================================")
        logger.info("==================================STARTING LOAD DATA=======================================")

        try:
            start_time = time.time()

            ##with target_engine.connect() as conn:
            ##    for index, table_name in enumerate(self.tables):
            ##        dfs = [pd.read_csv(r'D:\Kyrie\Pacmann\DataStorageManagement\Project\pacmann-bookstore\source_data\pipeline\temp\data' + f"\{table_name}.csv")]
##
            ##        for index, df in enumerate(dfs):
            ##            df.to_sql(
            ##                name=table_name,
            ##                con=target_engine,
            ##                schema="public",
            ##                if_exists="append",
            ##                index=False
            ##            )
            ##        
            ##        logger.info(f"LOAD '{table_name}' - SUCCESS")

            dfs: list[pd.DataFrame] = []

            for index,  table in enumerate(self.tables):
                df = pd.read_csv(r'D:\Kyrie\Pacmann\DataStorageManagement\Project\pacmann-bookstore\source_data\pipeline\temp\data' + f"\{table}.csv")
                dfs.append(df)

                logger.info(f"READ '{table}' - SUCCESS")
            
            logger.info("READ EXTRACTED TABLES - SUCCESS")

            for index, df in enumerate(dfs):
                df.to_sql(
                    name=self.tables[index],
                    con=target_engine,
                    schema="public",
                    if_exists="append",
                    index=False
                )

                logger.info(f"LOAD '{self.tables[index]}' - SUCCESS")
            logger.info("LOAD ALL DATA - SUCCESS")

            end_time = time.time()
            exe_time = end_time - start_time

            summary_data = {
                "timestamp": [datetime.datetime.now()],
                "task": ["Load"],
                "status": ["Success"],
                "execution_time": [exe_time]
            }
            summary = pd.DataFrame(summary_data)
            summary.to_csv(self.output().path, index=False, mode="a")
        except Exception as e:
            logger.error(f"LOAD ALL DATA - FAILED: {e}\n{traceback.format_exc()}")

            summary_data = {
                "timestamp": [datetime.datetime.now()],
                "task": ["Load"],
                "status": ["Failed"],
                "execution_time": [0]
            }
            summary = pd.DataFrame(summary_data)
            summary.to_csv(self.output().path, index=False, mode="a")

        logger.info("==================================ENDING LOAD DATA=======================================")

    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget(r'D:\Kyrie\Pacmann\DataStorageManagement\Project\pacmann-bookstore\source_data\pipeline\temp\data\loaded_data.csv')
