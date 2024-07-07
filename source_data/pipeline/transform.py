import time
import luigi
import datetime
import traceback
import pandas as pd
import subprocess as sp

from .load import Load
from .utils.log_config import copy

class GlobalParams(luigi.Config):
    CurrentTimestampParams = luigi.DateSecondParameter(default=datetime.datetime.now())

class DbtTask(luigi.Task):
    command = luigi.Parameter()

    current_timestamp = GlobalParams().CurrentTimestampParams

    def requires(self):
        pass
    
    def run(self):
        logger = copy(f"transform_{self.command}", self.current_timestamp)
        logger.info(f"==================================STARTING TRANSFORM DATA - dbt {self.command}=======================================")

        try:
            start_time = time.time()

            with open(r'D:\Kyrie\Pacmann\DataStorageManagement\Project\pacmann-bookstore' + f"\logs\transform_{self.command}\transform_{self.command}_{self.current_timestamp}.log", "a") as f:
                p1 = sp.run(
                    f"cd ./dwh_dbt/ && dbt {self.command}",
                    stdout=f,
                    stderr=sp.PIPE,
                    text=True,
                    shell=True,
                    check=True
                )

                if p1.returncode == 0:
                    logger.info(f"Success running dbt {self.command} process")
                else:
                    logger.error(f"Failed running dbt {self.command} process\n{traceback.format_exc()}")

            end_time = time.time()
            exe_time = end_time - start_time

            summary_data = {
                "timestamp": [datetime.datetime.now()],
                "task": [f"DBT {self.command}"],
                "status": ["Success"],
                "execution_time": [exe_time]
            }
            summary = pd.DataFrame(summary_data)
            summary.to_csv(self.output().path, index=False, mode="a")
        except Exception as e:
            logger.error(f"Failed process: {e}\n{traceback.format_exc()}")

            summary_data = {
                "timestamp": [datetime.datetime.now()],
                "task": [f"DBT {self.command}"],
                "status": ["Failed"],
                "execution_time": [0]
            }
            summary = pd.DataFrame(summary_data)
            summary.to_csv(self.output().path, index=False, mode="a")
        
        logger.info(f"==================================ENDING TRANSFORM DATA - dbt {self.command}=======================================")
    
    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget(r'D:\Kyrie\Pacmann\DataStorageManagement\Project\pacmann-bookstore\source_data\pipeline\temp\data\transformed_data.csv')

class DbtDebug(DbtTask):
    command = "debug"

    def requires(self):
        return Load()

class DbtDeps(DbtTask):
    command = "deps"

    def requires(self):
        return DbtDebug()

class DbtRun(DbtTask):
    command = "run"

    def requires(self):
        return DbtDeps()

class DbtSnapshot(DbtTask):
    command = "snapshot"

    def requires(self):
        return DbtRun()

class DbtTest(DbtTask):
    command = "test"

    def requires(self):
        return DbtSnapshot()