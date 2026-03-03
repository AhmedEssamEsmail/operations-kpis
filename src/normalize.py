"""Data normalization and cleaning module"""
import pandas as pd
import numpy as np
import re
import logging
from typing import Optional, Dict

logger = logging.getLogger(__name__)


class DataNormalizer:
    """Handles data cleaning and normalization"""
    
    # Null markers to clean
    NULL_MARKERS = ['#N/A', '#REF!', '#VALUE!', '#DIV/0!', '#NAME?', '#NULL!', 
                    'na', 'N/A', 'n/a', 'NA', 'null', 'NULL', 'None', '']
    
    def __init__(self, location_map: Optional[pd.DataFrame] = None):
        """Initialize normalizer with optional location mapping"""
        self.location_map = location_map
    
    def clean_null_markers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert various null markers to actual None/NaN"""
        df = df.copy()
        
        # Replace null markers with None
        for marker in self.NULL_MARKERS:
            df = df.replace(marker, np.nan)
        
        # Also handle whitespace-only strings
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].apply(lambda x: np.nan if isinstance(x, str) and x.strip() == '' else x)
        
        logger.info("Cleaned null markers from dataframe")
        return df
    
    def standardize_text(self, df: pd.DataFrame, columns: list) -> pd.DataFrame:
        """Standardize text fields (trim, normalize)"""
        df = df.copy()
        
        for col in columns:
            if col not in df.columns:
                continue
            
            if df[col].dtype == 'object':
                # Trim whitespace
                df[col] = df[col].str.strip()
                
                # Normalize multiple spaces to single space
                df[col] = df[col].str.replace(r'\s+', ' ', regex=True)
        
        logger.info(f"Standardized text for {len(columns)} columns")
        return df
    
    def normalize_location(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize city and area names"""
        df = df.copy()
        
        # Standardize city and area columns
        location_cols = ['city', 'area']
        for col in location_cols:
            if col in df.columns:
                # Basic normalization
                df[col] = df[col].str.strip()
                df[col] = df[col].str.title()  # Title case
        
        # Apply location mapping if available
        if self.location_map is not None and not self.location_map.empty:
            df = self._apply_location_mapping(df)
        
        logger.info("Normalized location fields")
        return df
    
    def _apply_location_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply location mapping table to standardize city/area names"""
        if 'warehouse' not in df.columns or 'city' not in df.columns:
            return df
        
        # Create lookup key
        df['_lookup_key'] = (
            df['warehouse'].str.lower().str.strip() + '|' +
            df['city'].fillna('').str.lower().str.strip() + '|' +
            df['area'].fillna('').str.lower().str.strip()
        )
        
        # Create mapping dictionary
        mapping_dict = {}
        for _, row in self.location_map.iterrows():
            key = (
                str(row.get('warehouse', '')).lower().strip() + '|' +
                str(row.get('original_city', '')).lower().strip() + '|' +
                str(row.get('original_area', '')).lower().strip()
            )
            mapping_dict[key] = {
                'city': row.get('standard_city'),
                'area': row.get('standard_area')
            }
        
        # Apply mapping
        for idx, row in df.iterrows():
            lookup_key = row['_lookup_key']
            if lookup_key in mapping_dict:
                mapped = mapping_dict[lookup_key]
                if pd.notna(mapped['city']):
                    df.at[idx, 'city'] = mapped['city']
                if pd.notna(mapped['area']):
                    df.at[idx, 'area'] = mapped['area']
        
        df = df.drop(columns=['_lookup_key'])
        logger.info(f"Applied location mapping to {len(mapping_dict)} entries")
        return df
    
    def detect_waiting_address(self, df: pd.DataFrame, address_column: str = 'delivery_address') -> pd.DataFrame:
        """Detect 'Waiting Address' (WA) flag from delivery address
        
        WA is identified by checking if delivery_address contains 'WA' or 'wa' 
        after 'Extra info:' in the address string.
        
        Examples:
        - "... Extra info: WA" -> has_wa = True
        - "... Extra info: wa" -> has_wa = True
        - "... Extra info: Ring the bell" -> has_wa = False
        """
        df = df.copy()
        
        if address_column not in df.columns:
            logger.warning(f"Column {address_column} not found, skipping WA detection")
            df['has_waiting_address'] = False
            return df
        
        def check_wa(address):
            """Check if address has WA (Waiting Address) flag"""
            if pd.isna(address):
                return False
            
            address_str = str(address).lower()
            
            # Look for "extra info:" followed by "wa"
            # Pattern: "extra info:" followed by optional whitespace/colon, then "wa"
            pattern = r'extra\s+info\s*:.*?\bwa\b'
            
            if re.search(pattern, address_str):
                return True
            
            return False
        
        df['has_waiting_address'] = df[address_column].apply(check_wa)
        
        wa_count = df['has_waiting_address'].sum()
        logger.info(f"Detected {wa_count} parcels with Waiting Address (WA) flag")
        
        return df
    
    def deduplicate(self, df: pd.DataFrame, key_column: str = 'parcel_id', 
                   updated_column: Optional[str] = None) -> pd.DataFrame:
        """Deduplicate records by key column
        
        Args:
            df: DataFrame to deduplicate
            key_column: Column to use as unique key
            updated_column: Optional column to use for keeping latest record
        
        Returns:
            Deduplicated DataFrame
        """
        df = df.copy()
        
        initial_count = len(df)
        
        if key_column not in df.columns:
            logger.warning(f"Key column {key_column} not found, skipping deduplication")
            return df
        
        # Check for duplicates
        duplicates = df[df.duplicated(subset=[key_column], keep=False)]
        
        if len(duplicates) == 0:
            logger.info("No duplicates found")
            return df
        
        # If updated_column exists, keep latest record
        if updated_column and updated_column in df.columns:
            df = df.sort_values(by=[key_column, updated_column], ascending=[True, False])
            df = df.drop_duplicates(subset=[key_column], keep='first')
        else:
            # Keep last occurrence
            df = df.drop_duplicates(subset=[key_column], keep='last')
        
        final_count = len(df)
        removed = initial_count - final_count
        
        logger.info(f"Deduplication: removed {removed} duplicate records (kept {final_count})")
        
        return df
    
    def normalize_warehouse_name(self, df: pd.DataFrame, warehouse_column: str = 'warehouse') -> pd.DataFrame:
        """Normalize warehouse names to match config
        
        Examples:
        - "Kuwait warehouse" -> "Kuwait"
        - "kuwait warehouse" -> "Kuwait"
        - "Riyadh Warehouse" -> "Riyadh"
        """
        df = df.copy()
        
        if warehouse_column not in df.columns:
            return df
        
        # Remove common suffixes and normalize
        df[warehouse_column] = df[warehouse_column].str.replace(r'\s+warehouse\s*$', '', case=False, regex=True)
        df[warehouse_column] = df[warehouse_column].str.strip()
        df[warehouse_column] = df[warehouse_column].str.title()
        
        logger.info(f"Normalized warehouse names in column: {warehouse_column}")
        return df


def normalize_dataframe(df: pd.DataFrame, 
                       location_map: Optional[pd.DataFrame] = None,
                       text_columns: Optional[list] = None,
                       key_column: str = 'parcel_id',
                       detect_wa: bool = True) -> pd.DataFrame:
    """Apply all normalization steps to a dataframe
    
    Args:
        df: DataFrame to normalize
        location_map: Optional location mapping table
        text_columns: Columns to standardize text
        key_column: Column to use for deduplication
        detect_wa: Whether to detect Waiting Address flag
    
    Returns:
        Normalized DataFrame
    """
    normalizer = DataNormalizer(location_map=location_map)
    
    # Clean null markers
    df = normalizer.clean_null_markers(df)
    
    # Standardize text columns
    if text_columns:
        df = normalizer.standardize_text(df, text_columns)
    
    # Normalize warehouse names
    df = normalizer.normalize_warehouse_name(df)
    
    # Normalize locations
    df = normalizer.normalize_location(df)
    
    # Detect Waiting Address flag
    if detect_wa:
        df = normalizer.detect_waiting_address(df)
    
    # Deduplicate
    df = normalizer.deduplicate(df, key_column=key_column)
    
    return df
