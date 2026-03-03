"""Data ingestion module for Operations KPIs"""
import pandas as pd
import yaml
import logging
from pathlib import Path
from typing import Dict, List, Optional, Union
from dateutil import parser as date_parser
import chardet

from src.types import SchemaMapping

logger = logging.getLogger(__name__)


class DataIngester:
    """Handles data ingestion from multiple file formats"""
    
    def __init__(self, schema_map_path: Optional[Path] = None):
        """Initialize data ingester with optional schema mapping"""
        self.schema_mapping = self._load_schema_mapping(schema_map_path)
    
    def _load_schema_mapping(self, schema_map_path: Optional[Path]) -> Optional[SchemaMapping]:
        """Load schema mapping from YAML file"""
        if not schema_map_path or not schema_map_path.exists():
            logger.warning("No schema mapping file found, using column names as-is")
            return None
        
        with open(schema_map_path, 'r', encoding='utf-8') as f:
            mapping_data = yaml.safe_load(f)
        
        return SchemaMapping(field_mappings=mapping_data)
    
    def detect_encoding(self, file_path: Path) -> str:
        """Detect file encoding"""
        with open(file_path, 'rb') as f:
            result = chardet.detect(f.read(10000))
        return result['encoding'] or 'utf-8'
    
    def read_csv(self, file_path: Path, **kwargs) -> pd.DataFrame:
        """Read CSV file with encoding detection"""
        encoding = self.detect_encoding(file_path)
        try:
            df = pd.read_csv(file_path, encoding=encoding, **kwargs)
            logger.info(f"Successfully read CSV: {file_path.name} ({len(df)} rows)")
            return df
        except Exception as e:
            logger.error(f"Error reading CSV {file_path}: {e}")
            raise
    
    def read_excel(self, file_path: Path, sheet_name: Union[str, int] = 0, **kwargs) -> pd.DataFrame:
        """Read Excel file"""
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl', **kwargs)
            logger.info(f"Successfully read Excel: {file_path.name} ({len(df)} rows)")
            return df
        except Exception as e:
            logger.error(f"Error reading Excel {file_path}: {e}")
            raise
    
    def read_file(self, file_path: Path, **kwargs) -> pd.DataFrame:
        """Auto-detect and read file format"""
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        suffix = file_path.suffix.lower()
        
        if suffix == '.csv':
            return self.read_csv(file_path, **kwargs)
        elif suffix in ['.xlsx', '.xls']:
            return self.read_excel(file_path, **kwargs)
        else:
            raise ValueError(f"Unsupported file format: {suffix}")
    
    def apply_schema_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply schema mapping to standardize column names"""
        if not self.schema_mapping:
            return df
        
        column_mapping = self.schema_mapping.map_columns(df.columns.tolist())
        
        if column_mapping:
            df = df.rename(columns=column_mapping)
            logger.info(f"Applied schema mapping: {len(column_mapping)} columns mapped")
        
        return df
    
    def parse_dates_flexible(self, df: pd.DataFrame, date_columns: List[str]) -> pd.DataFrame:
        """Parse dates with flexible format detection"""
        for col in date_columns:
            if col not in df.columns:
                continue
            
            # Skip if already datetime
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                continue
            
            try:
                # Try pandas default parser first
                df[col] = pd.to_datetime(df[col], errors='coerce')
                
                # For any that failed, try dateutil parser
                mask = df[col].isna() & df[col].notna()
                if mask.any():
                    df.loc[mask, col] = df.loc[mask, col].apply(
                        lambda x: date_parser.parse(str(x), fuzzy=True) if pd.notna(x) else pd.NaT
                    )
                
                logger.info(f"Parsed date column: {col}")
            except Exception as e:
                logger.warning(f"Could not parse date column {col}: {e}")
        
        return df


class DatasetLoader:
    """Loads specific datasets with domain logic"""
    
    def __init__(self, ingester: DataIngester):
        self.ingester = ingester
    
    def load_delivery_details(self, file_path: Path) -> pd.DataFrame:
        """Load Delivery Details dataset"""
        df = self.ingester.read_file(file_path)
        df = self.ingester.apply_schema_mapping(df)
        
        # Parse date columns
        date_cols = ['delivery_date', 'order_date', 'parcel_delivery_date']
        df = self.ingester.parse_dates_flexible(df, date_cols)
        
        logger.info(f"Loaded Delivery Details: {len(df)} records")
        return df
    
    def load_parcel_logs(self, file_path: Path) -> pd.DataFrame:
        """Load Parcel Logs dataset (status transitions)"""
        df = self.ingester.read_file(file_path)
        df = self.ingester.apply_schema_mapping(df)
        
        # Parse date columns
        date_cols = ['parcel_date', 'order_date']
        df = self.ingester.parse_dates_flexible(df, date_cols)
        
        logger.info(f"Loaded Parcel Logs: {len(df)} records")
        return df
    
    def load_collectors_report(self, file_path: Path) -> pd.DataFrame:
        """Load Collectors Report dataset"""
        df = self.ingester.read_file(file_path)
        df = self.ingester.apply_schema_mapping(df)
        
        # Parse date columns
        date_cols = ['parcel_date', 'start_time', 'finish_time']
        df = self.ingester.parse_dates_flexible(df, date_cols)
        
        logger.info(f"Loaded Collectors Report: {len(df)} records")
        return df
    
    def load_prepare_report(self, file_path: Path) -> pd.DataFrame:
        """Load Prepare Report dataset"""
        df = self.ingester.read_file(file_path)
        df = self.ingester.apply_schema_mapping(df)
        
        # Parse date columns
        date_cols = ['parcel_date', 'start_time', 'finish_time']
        df = self.ingester.parse_dates_flexible(df, date_cols)
        
        logger.info(f"Loaded Prepare Report: {len(df)} records")
        return df
    
    def load_items_per_order(self, file_path: Path) -> pd.DataFrame:
        """Load Items Per Order Report dataset"""
        df = self.ingester.read_file(file_path)
        df = self.ingester.apply_schema_mapping(df)
        
        # Parse date columns
        date_cols = ['order_date']
        df = self.ingester.parse_dates_flexible(df, date_cols)
        
        logger.info(f"Loaded Items Per Order: {len(df)} records")
        return df
    
    def load_freshdesk_data(self, file_path: Path) -> pd.DataFrame:
        """Load FreshDesk Data dataset (tickets/issues)"""
        df = self.ingester.read_file(file_path)
        df = self.ingester.apply_schema_mapping(df)
        
        # Parse date columns
        date_cols = ['order_date']
        df = self.ingester.parse_dates_flexible(df, date_cols)
        
        logger.info(f"Loaded FreshDesk Data: {len(df)} records")
        return df
