from sqlalchemy import create_engine
import warnings
warnings.filterwarnings('ignore')
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

def db_connection():
    try:
        src_database = os.getenv("SRC_POSTGRES_DB")
        src_host = os.getenv("SRC_POSTGRES_HOST")
        src_user = os.getenv("SRC_POSTGRES_USER")
        src_password = os.getenv("SRC_POSTGRES_PASSWORD")
        src_port = os.getenv("SRC_POSTGRES_PORT")

        target_database = os.getenv("TARGET_POSTGRES_DB")
        target_host = os.getenv("TARGET_POSTGRES_HOST")
        target_user = os.getenv("TARGET_POSTGRES_USER")
        target_password = os.getenv("TARGET_POSTGRES_PASSWORD")
        target_port = os.getenv("TARGET_POSTGRES_PORT")


        
        
        src_conn = f'postgresql://{src_user}:{src_password}@{src_host}:{src_port}/{src_database}'
        target_conn = f'postgresql://{target_user}:{target_password}@{target_host}:{target_port}/{target_database}'
        
        src_engine = create_engine(src_conn)
        target_engine = create_engine(target_conn)

        ##target_conn = create_engine( f"postgresql://{target_user}:{target_password}@{target_host}:{target_port}/{target_database}")
        
        return src_engine, target_engine

    except Exception as e:
        print(f"Error: {e}")
        return None