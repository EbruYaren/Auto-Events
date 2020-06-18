from src.config import REDSHIFT_ETL_URI
from sqlalchemy import create_engine

REDSHIFT_ETL = create_engine(REDSHIFT_ETL_URI)