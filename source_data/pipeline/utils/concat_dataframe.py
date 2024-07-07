import pandas as pd
from dotenv import load_dotenv
import os

# Define root dir project
ROOT_DIR = os.getenv("DIR_ROOT_PROJECT")

def concat_dataframes(df1, df2):
    """
    Concatenates two DataFrames along the rows.

    Parameters:
    - df1, df2: DataFrames

    Returns:
    - concatenated_df: DataFrame
    """
    concatenated_df = pd.concat([df1, df2], ignore_index=True)
    concatenated_df.to_csv(r'D:\Kyrie\Pacmann\DataStorageManagement\Project\pacmann-bookstore\pipeline_summary.csv', index = False)