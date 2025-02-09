"""
JOLTS Data Pipeline v2.0
Error-resilient architecture with expansion capabilities
Inspired by Warren Buffett's margin of safety principle
"""

# Core Infrastructure
import logging
import requests
from typing import Optional, Dict
from datetime import datetime
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine  # Add missing import
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.engine.base import Engine
# Add this import at the top of your file
from contextlib import contextmanager
import os
import json

# Security Configuration (Add right after imports)
DB_CONFIG = {
    "user": os.getenv('DB_USER', 'fallback_user'),
    "pass": os.getenv('DB_PASS', 'complex_default'),
    "host": os.getenv('DB_HOST', 'localhost'),
    "name": os.getenv('DB_NAME', 'economy_indicator_db')
}
DB_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['pass']}@{DB_CONFIG['host']}/{DB_CONFIG['name']}"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('pipeline.log'), logging.StreamHandler()]
)

Base = declarative_base()

class EconomicData(Base):
    """Data storage foundation following Charlie Munger's latticework model"""
    __tablename__ = 'economic_metrics'
    
    id = sa.Column(sa.Integer, primary_key=True)
    metric_date = sa.Column(sa.DateTime, nullable=False, index=True)
    job_openings = sa.Column(sa.Float)
    created_at = sa.Column(sa.DateTime, default=datetime.now)
    updated_at = sa.Column(sa.DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        sa.UniqueConstraint('metric_date', name='uq_metric_date'),
    )

class DatabaseManager:
    """Manages database connections and initialization."""
    def __init__(self, db_url: str = DB_URL):  # Use configured default
        try:
            self.engine = create_engine(db_url,
                                        pool_size=20,
                                        max_overflow=10,
                                        pool_pre_ping=True)
            self.Session = sessionmaker(bind=self.engine)  # Create session factory

        except Exception as e:
            logging.error(f"Database connection failed: {e}")
            raise
   
    def initialize_db(self):
        """Initializes the database by creating all tables."""
        try:
            Base.metadata.create_all(self.engine)
            logging.info("Database initialized successfully.")
        except Exception as e:
            logging.error(f"Error initializing database: {e}")
            raise
    
    @contextmanager
    def session_scope(self):
        """Provides a transactional scope for database operations."""
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logging.error(f"Database transaction error: {e}")
            raise
        finally:
            session.close()

    # Add to DatabaseManager
    def query_usage_metrics(self):
        """Track API usage against BLS limits"""
        with self.session_scope() as session:
            usage = session.query(
                func.count(EconomicData.metric_date)
            ).scalar()
            remaining = 500 - usage  # Daily limit
            logging.info(f"API Quota: {remaining} requests remaining")


class JOLTSDataFetcher:
    """Enhanced with BLS API v2.4 compliance and error resilience"""
    def __init__(self, api_config: Dict):
        self.base_url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
        self.api_key = api_config.get('BLS_JOLTS_KEY') if api_config else None
        self.headers = {'Content-type': 'application/json'}
        # Validate API key presence
        if not self.api_key:
            logging.critical("Missing BLS API key in configuration")
            raise ValueError("API key required for BLS access")

    def fetch_data(self, start_year: int = None, end_year: int = None) -> Optional[pd.DataFrame]:
        """Robust data fetching with multiple validation layers"""
        try:
            today = datetime.today()
            start_year = start_year or today.year - 10  # BLS default limit
            end_year = end_year or today.year

            # Updated JOLTS series ID structure (post-2020)
            series_id = 'JTS000000000000000JOR'  # Verify against current specs
            payload = {
                "seriesid": [series_id],
                "startyear": str(start_year),
                "endyear": str(end_year),
                "registrationkey": self.api_key,
                "aspects": "true"  # New v2.4 parameter
            }

            response = requests.post(
                self.base_url,
                data=json.dumps(payload),
                headers=self.headers,
                timeout=15
            )
            response.raise_for_status()
            
            return self._parse_response(response.json())

        except requests.exceptions.RequestException as e:
            logging.error(f"API Connection Error: {e}")
            return None

    def _parse_response(self, raw_data: Dict) -> pd.DataFrame:
        """Defensive parsing with multiple validation checks"""
        try:
            # Check API response status first
            if raw_data.get('status') != 'REQUEST_SUCCEEDED':
                error_msg = raw_data.get('message', ['Unknown error'])[0]
                logging.error(f"BLS API Error: {error_msg}")
                return None

            # Process results data
            results = raw_data.get('Results', {})
            series_list = results.get('series', [])
            
            if not series_list:
                logging.warning("No series data found in API response")
                return pd.DataFrame()

            # Validate data structure
            series_data = series_list[0].get('data', [])
            if not series_data:
                logging.info("No records found for given parameters")
                return pd.DataFrame()

            # Create dataframe with type safety
            df = pd.DataFrame(series_data).assign(
                metric_date=lambda x: pd.to_datetime(
                    x['year'] + '-' + x['period'].str[1:] + '-01',
                    errors='coerce'
                ),
                job_openings=lambda x: pd.to_numeric(x['value'], errors='coerce')
            )

            # Quality control checks AFTER dataframe creation
            if df['job_openings'].isnull().mean() > 0.2:
                logging.warning("High null rate in job openings data")

            return df[['metric_date', 'job_openings']].dropna()

        except KeyError as e:
            logging.error(f"Data structure mismatch: {e}")
            return None

# Expansion Interface
class DataPipelinePlugin:
    """Bill Gates' extensibility pattern"""
    def pre_process(self, data: pd.DataFrame) -> pd.DataFrame:
        return data
        
    def post_process(self, data: pd.DataFrame) -> pd.DataFrame:
        return data

# Implementation
if __name__ == "__main__":
    # Initialize with Robert Kiyosaki's simplicity principle
    db_mgr = DatabaseManager()

    db_mgr.initialize_db()

    def load_config():
        try:
            with open('./config.json', 'r') as file:
                config = json.load(file)
                # Directly return the API key from root of config
                return {"BLS_JOLTS_KEY": config["BLS_JOLTS_KEY"]}
        except KeyError:
            logging.error("Missing BLS_JOLTS_KEY in config.json")
            raise
        except FileNotFoundError:
            logging.error("config.json file not found in project root")
            raise


    fetcher = JOLTSDataFetcher(api_config=load_config())
        
    with db_mgr.session_scope() as session:
        raw_data = fetcher.fetch_data()
        if raw_data is not None:
            existing_dates = session.query(EconomicData.metric_date).all()
            new_data = raw_data[~raw_data['metric_date'].isin(existing_dates)]
            
            session.bulk_insert_mappings(
                EconomicData,
                new_data.to_dict(orient='records')
            )
            logging.info(f"Inserted {len(new_data)} new records")
        pass
