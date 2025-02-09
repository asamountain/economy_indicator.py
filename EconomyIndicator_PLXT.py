"""
JOLTS Data Pipeline v2.0
Error-resilient architecture with expansion capabilities
"""

# Core Infrastructure
import logging
import requests
from typing import Optional, Dict
from datetime import datetime
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.engine.base import Engine
from sqlalchemy.dialects.postgresql import insert
from contextlib import contextmanager
import os
import json
import matplotlib.pyplot as plt

Base = declarative_base()

# Database Configuration
DB_CONFIG = {
    "user": os.getenv('DB_USER'),
    "pass": os.getenv('DB_PASS'),
    "host": os.getenv('DB_HOST'),
    "name": os.getenv('DB_NAME')
}
DB_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['pass']}@{DB_CONFIG['host']}/{DB_CONFIG['name']}"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('pipeline.log'), logging.StreamHandler()]
)

class EconomicData(Base):
    """Core economic metrics storage model"""
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
    """Central database connection handler"""
    def __init__(self, db_url: str = DB_URL):
        try:
            self.engine = create_engine(db_url,
                                        pool_size=20,
                                        max_overflow=10,
                                        pool_pre_ping=True)
            self.Session = sessionmaker(bind=self.engine)
        except Exception as e:
            logging.error(f"Database connection failed: {e}")
            raise
   
    def initialize_db(self):
        """Create database schema"""
        try:
            Base.metadata.create_all(self.engine)
            logging.info("Database initialized successfully.")
        except Exception as e:
            logging.error(f"Error initializing database: {e}")
            raise
    
    @contextmanager
    def session_scope(self):
        """Transactional session management"""
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

    def query_usage_metrics(self):
        """Track API usage limits"""
        with self.session_scope() as session:
            usage = session.query(
                sa.func.count(EconomicData.metric_date)
            ).scalar()
            remaining = 500 - usage
            logging.info(f"API Quota: {remaining} requests remaining")

BLS_API_KEY = os.getenv("BLS_JOLTS_KEY")
if not BLS_API_KEY:
    raise ValueError("Set BLS_JOLTS_KEY environment variable")

class JOLTSDataFetcher:
    """BLS API data acquisition handler"""
    def __init__(self):
        self.base_url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
        self.headers = {'Content-type': 'application/json'}
        self.api_key = BLS_API_KEY

        if not self.api_key:
            logging.critical("Missing BLS API key")
            raise ValueError("API key required for BLS access")

    def fetch_data(self, start_year: int = None, end_year: int = None) -> Optional[pd.DataFrame]:
        """Retrieve JOLTS data from BLS API"""
        try:
            today = datetime.today()
            start_year = start_year or today.year - 10
            end_year = end_year or today.year

            payload = {
                "seriesid": ['JTS000000000000000JOR'],
                "startyear": str(start_year),
                "endyear": str(end_year),
                "registrationkey": self.api_key,
                "aspects": "true"
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
        """Parses BLS API response with robust error handling"""
        df = pd.DataFrame(columns=['metric_date', 'job_openings'])  # Initialize empty DF
    
        try:
            if not raw_data or raw_data.get('status') != 'REQUEST_SUCCEEDED':
                error_msg = raw_data.get('message', ['Unknown error'])[0] if raw_data else 'Empty response'
                logging.error(f"BLS API Error: {error_msg}")
                return df

            series_list = raw_data.get('Results', {}).get('series', [])
            if not series_list:
                logging.warning("No series data found in API response")
                return df

            series_data = series_list[0].get('data', [])
            if not series_data:
                logging.info("No records found for given parameters")
                return df

            # Create DataFrame with validation
            df = pd.DataFrame(series_data).assign(
                metric_date=lambda x: pd.to_datetime(
                    x['year'] + '-' + x['period'].str[1:] + '-01',
                    errors='coerce'
                ),
                job_openings=lambda x: pd.to_numeric(x['value'], errors='coerce')
            ).dropna()

        except (KeyError, IndexError, AttributeError) as e:
            logging.error(f"Data parsing failed: {str(e)}")
        
        return df[['metric_date', 'job_openings']]


class DataPipelinePlugin:
    """Extensible data processing interface"""
    def pre_process(self, data: pd.DataFrame) -> pd.DataFrame:
        return data
        
    def post_process(self, data: pd.DataFrame) -> pd.DataFrame:
        return data

if __name__ == "__main__":
    db_mgr = DatabaseManager()
    try:
        db_mgr.initialize_db()
    except Exception as e:
        logging.critical(f"Database initialization failed: {e}")
        exit(1)    
    
    fetcher = JOLTSDataFetcher()

    try:
        with db_mgr.session_scope() as session:
            # Data insertion
            raw_data = fetcher.fetch_data()
            if raw_data is not None:
                df = pd.DataFrame()  # Initialize here for scope
                try:
                    stmt = insert(EconomicData.__table__).values(
                        raw_data.to_dict(orient='records')
                    ).on_conflict_do_nothing(
                        index_elements=['metric_date']
                    )
                    result = session.execute(stmt)
                    logging.info(f"Inserted {result.rowcount} new records")

                    # Query and display data
                    df = pd.read_sql(
                        """SELECT * FROM economic_metrics 
                           ORDER BY metric_date DESC LIMIT 5""",
                        con=session.connection()
                    )
                except Exception as e:
                    logging.error(f"Database operation failed: {e}")

                # Display latest records from SQLAlchemy query
                latest_data = session.query(EconomicData).order_by(
                    EconomicData.metric_date.desc()
                ).limit(5).all()
                
                if latest_data:
                    print("\nLatest 5 JOLTS Records:")
                    for record in latest_data:
                        print(f"{record.metric_date.date()}: {record.job_openings}")
                else:
                    logging.info("No records found in database")

                # Visualization handling
                if not df.empty:
                    try:
                        df['job_openings'] = pd.to_numeric(df['job_openings'], errors='coerce')
                        df = df.dropna(subset=['job_openings'])
                        
                        if not df.empty:
                            print("\nRecent JOLTS Data:")
                            print(df[['metric_date', 'job_openings']].head(10))
                            
                            ax = df.set_index('metric_date')['job_openings'].plot(
                                title='30-Day JOLTS Trend',
                                figsize=(12, 6),
                                grid=True,
                                style='o-'
                            )
                            ax.set_ylabel('Job Openings')
                            plt.tight_layout()
                            plt.show()
                        else:
                            logging.info("No numeric data available for visualization")
                    except Exception as e:
                        logging.error(f"Visualization error: {e}")

    except Exception as e:
        logging.error(f"Main execution error: {e}")

