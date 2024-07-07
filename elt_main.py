import luigi
import sentry_sdk
import pandas as pd
import os
from dotenv import load_dotenv

from source_data.pipeline.extract import Extract
from source_data.pipeline.load import Load
from source_data.pipeline.transform import DbtDebug, DbtDeps, DbtRun, DbtSnapshot, DbtTest
from source_data.pipeline.utils.concat_dataframe import concat_dataframes
from source_data.pipeline.utils.log_config import copy_log
##from pipeline.utils.delete_temp_data import delete_temp


# Load environment variables from .env file
load_dotenv()

# Read env variables
DIR_ROOT_PROJECT = os.getenv("DIR_ROOT_PROJECT")
DIR_TEMP_LOG = os.getenv("DIR_TEMP_LOG")
DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")
DIR_LOG = os.getenv("DIR_LOG")
SENTRY_DSN = os.getenv("SENTRY_DSN")

# Track the error using sentry
##sentry_sdk.init(
##    dsn = f"{SENTRY_DSN}"
##)


# Execute the functions when the script is run
if __name__ == "__main__":
    # Build the task
    luigi.build([Extract(),
                 Load()
                 ##DbtDebug(),
                 ##DbtDeps(), 
                 ##DbtRun(), 
                 ##DbtSnapshot(), 
                 ##DbtTest()
                 ])
    
   