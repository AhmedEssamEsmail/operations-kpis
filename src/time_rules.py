"""Time rules and timezone handling module"""
import pandas as pd
import pytz
from datetime import datetime, time, timedelta
from typing import Dict, Optional, Tuple
import logging

from src.types import WarehouseConfig

logger = logging.getLogger(__name__)


class TimestampExtractor:
    """Extracts timestamps from Parcel Logs status transitions"""
    
    STATUS_MAPPING = {
        'picked_at': ['Collecting', 'collecting'],
        'packed_at': ['Prepare', 'prepare', 'Preparing'],
        'out_for_delivery_at': ['On The Way', 'on the way', 'Ready For Delivery'],
        'delivered_at': ['Delivered', 'delivered']
    }
    
    def __init__(self):
        pass
    
    def extract_timestamps(self, parcel_logs: pd.DataFrame) -> pd.DataFrame:
        """Extract key timestamps from parcel logs
        
        Args:
            parcel_logs: DataFrame with columns: parcel_id, parcel_date, parcelStatus_name
        
        Returns:
            DataFrame with parcel_id and extracted timestamps
        """
        if parcel_logs.empty:
            return pd.DataFrame()
        
        # Ensure required columns exist
        required_cols = ['parcel_id', 'parcel_date', 'parcel_status']
        for col in required_cols:
            if col not in parcel_logs.columns:
                logger.error(f"Missing required column: {col}")
                return pd.DataFrame()
        
        # Sort by parcel_id and timestamp to get chronological order
        parcel_logs = parcel_logs.sort_values(['parcel_id', 'parcel_date'])
        
        # Extract timestamps for each status
        timestamps = {}
        
        for timestamp_field, status_names in self.STATUS_MAPPING.items():
            # Find first occurrence of each status per parcel
            mask = parcel_logs['parcel_status'].isin(status_names)
            first_occurrence = parcel_logs[mask].groupby('parcel_id')['parcel_date'].first()
            timestamps[timestamp_field] = first_occurrence
        
        # Combine into single DataFrame
        result = pd.DataFrame(timestamps)
        result = result.reset_index()
        
        logger.info(f"Extracted timestamps for {len(result)} parcels")
        return result
    
    def forward_fill_missing_timestamps(self, df: pd.DataFrame, 
                                       timestamp_columns: list,
                                       max_lookback: int = 2) -> pd.DataFrame:
        """Forward-fill missing timestamps from previous rows
        
        If a timestamp is missing, use the value from the row above it.
        If that's also missing, look up to max_lookback rows back.
        
        Args:
            df: DataFrame with timestamp columns
            timestamp_columns: List of timestamp column names to fill
            max_lookback: Maximum number of rows to look back (default: 2)
        
        Returns:
            DataFrame with filled timestamps
        """
        df = df.copy()
        
        for col in timestamp_columns:
            if col not in df.columns:
                continue
            
            # Apply forward fill with limit
            df[col] = df[col].fillna(method='ffill', limit=max_lookback)
        
        logger.info(f"Applied forward-fill to {len(timestamp_columns)} timestamp columns")
        return df


class TimezoneHandler:
    """Handles timezone localization and conversion"""
    
    def __init__(self, warehouse_configs: Dict[str, WarehouseConfig]):
        """Initialize with warehouse configurations"""
        self.warehouse_configs = warehouse_configs
    
    def localize_timestamps(self, df: pd.DataFrame, 
                           timestamp_columns: list,
                           warehouse_column: str = 'warehouse') -> pd.DataFrame:
        """Localize timestamps to warehouse timezone and create UTC versions
        
        Creates both *_local and *_utc versions of each timestamp column.
        
        Args:
            df: DataFrame with timestamp columns
            timestamp_columns: List of timestamp column names
            warehouse_column: Column containing warehouse name
        
        Returns:
            DataFrame with localized timestamps
        """
        df = df.copy()
        
        if warehouse_column not in df.columns:
            logger.error(f"Warehouse column {warehouse_column} not found")
            return df
        
        for col in timestamp_columns:
            if col not in df.columns:
                continue
            
            # Create local and UTC columns
            local_col = f"{col}_local"
            utc_col = f"{col}_utc"
            
            df[local_col] = pd.NaT
            df[utc_col] = pd.NaT
            
            # Process each warehouse separately
            for warehouse_name, config in self.warehouse_configs.items():
                mask = df[warehouse_column] == warehouse_name
                
                if not mask.any():
                    continue
                
                try:
                    tz = pytz.timezone(config.timezone)
                    
                    # Localize to warehouse timezone
                    df.loc[mask, local_col] = df.loc[mask, col].apply(
                        lambda x: tz.localize(x) if pd.notna(x) and x.tzinfo is None else x
                    )
                    
                    # Convert to UTC
                    df.loc[mask, utc_col] = df.loc[mask, local_col].apply(
                        lambda x: x.astimezone(pytz.UTC) if pd.notna(x) else pd.NaT
                    )
                    
                except Exception as e:
                    logger.error(f"Error localizing timestamps for {warehouse_name}: {e}")
        
        logger.info(f"Localized {len(timestamp_columns)} timestamp columns")
        return df
    
    def calculate_durations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate phase durations in minutes
        
        Calculates:
        - collect_duration_min = picked_at - order_created_at
        - pack_duration_min = packed_at - picked_at
        - dispatch_duration_min = out_for_delivery_at - packed_at
        - delivery_duration_min = delivered_at - out_for_delivery_at
        - end_to_end_min = delivered_at - order_created_at
        
        Args:
            df: DataFrame with *_local timestamp columns
        
        Returns:
            DataFrame with duration columns
        """
        df = df.copy()
        
        # Define duration calculations
        durations = {
            'collect_duration_min': ('picked_at_local', 'order_created_at_local'),
            'pack_duration_min': ('packed_at_local', 'picked_at_local'),
            'dispatch_duration_min': ('out_for_delivery_at_local', 'packed_at_local'),
            'delivery_duration_min': ('delivered_at_local', 'out_for_delivery_at_local'),
            'end_to_end_min': ('delivered_at_local', 'order_created_at_local')
        }
        
        for duration_col, (end_col, start_col) in durations.items():
            if end_col in df.columns and start_col in df.columns:
                # Calculate duration in minutes
                df[duration_col] = (df[end_col] - df[start_col]).dt.total_seconds() / 60
                
                # Set negative durations to None (data quality issue)
                df.loc[df[duration_col] < 0, duration_col] = None
        
        logger.info("Calculated phase durations")
        return df


class ShiftCutoffRules:
    """Handles shift start, cutoff time, and Ramadan rules"""
    
    def __init__(self, warehouse_configs: Dict[str, WarehouseConfig]):
        """Initialize with warehouse configurations"""
        self.warehouse_configs = warehouse_configs
    
    def calculate_adjusted_start_time(self, df: pd.DataFrame,
                                     order_time_column: str = 'order_created_at_local',
                                     warehouse_column: str = 'warehouse') -> pd.DataFrame:
        """Calculate adjusted SLA start time based on shift/cutoff rules
        
        Rules:
        - If order created before shift_start: start = same_day shift_start
        - If order created after cutoff_time: start = next_day shift_start
        - Otherwise: start = order_created_at
        
        Args:
            df: DataFrame with order timestamps
            order_time_column: Column with order creation time (localized)
            warehouse_column: Column with warehouse name
        
        Returns:
            DataFrame with adjusted_start_time column
        """
        df = df.copy()
        
        if order_time_column not in df.columns:
            logger.error(f"Order time column {order_time_column} not found")
            return df
        
        df['adjusted_start_time'] = pd.NaT
        df['is_ramadan'] = False
        
        for warehouse_name, config in self.warehouse_configs.items():
            mask = df[warehouse_column] == warehouse_name
            
            if not mask.any():
                continue
            
            for idx in df[mask].index:
                order_time = df.at[idx, order_time_column]
                
                if pd.isna(order_time):
                    continue
                
                # Check if Ramadan period
                is_ramadan = config.is_ramadan_period(order_time)
                df.at[idx, 'is_ramadan'] = is_ramadan
                
                # Get shift times (Ramadan-aware)
                shift_start = config.get_shift_start_time(is_ramadan)
                cutoff_time = config.get_cutoff_time(is_ramadan)
                
                order_time_only = order_time.time()
                order_date = order_time.date()
                
                # Apply rules
                if order_time_only < shift_start:
                    # Before shift: start at shift_start same day
                    adjusted = datetime.combine(order_date, shift_start)
                    adjusted = pytz.timezone(config.timezone).localize(adjusted)
                elif order_time_only > cutoff_time:
                    # After cutoff: start at shift_start next day
                    next_day = order_date + timedelta(days=1)
                    adjusted = datetime.combine(next_day, shift_start)
                    adjusted = pytz.timezone(config.timezone).localize(adjusted)
                else:
                    # Normal: start at order time
                    adjusted = order_time
                
                df.at[idx, 'adjusted_start_time'] = adjusted
        
        logger.info("Calculated adjusted start times with shift/cutoff rules")
        return df
