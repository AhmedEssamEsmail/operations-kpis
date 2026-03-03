"""SLA calculation and late reason tagging engine"""
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

from src.types import WarehouseConfig

logger = logging.getLogger(__name__)


class SLAEngine:
    """Calculates SLA status and tags late reasons"""
    
    # Late reason priority (higher = more important)
    LATE_REASON_PRIORITY = {
        'missing_events': 1,
        'unknown_zone': 2,
        'before_shift': 3,
        'after_cutoff': 4,
        'delivery_delay': 5,
        'damaged_item': 6,
        'customer_unavailable': 7,
        'address_issue': 8,
        'exceeded_sla': 9
    }
    
    def __init__(self, warehouse_configs: Dict[str, WarehouseConfig]):
        """Initialize with warehouse configurations"""
        self.warehouse_configs = warehouse_configs
    
    def calculate_expected_delivery(self, df: pd.DataFrame,
                                   adjusted_start_col: str = 'adjusted_start_time',
                                   sla_hours_col: str = 'sla_hours') -> pd.DataFrame:
        """Calculate expected delivery time
        
        expected_delivery_at_local = adjusted_start + sla_hours
        
        Args:
            df: DataFrame with adjusted_start_time and sla_hours
            adjusted_start_col: Column with adjusted SLA start time
            sla_hours_col: Column with SLA hours
        
        Returns:
            DataFrame with expected_delivery_at_local and expected_delivery_at_utc
        """
        df = df.copy()
        
        if adjusted_start_col not in df.columns or sla_hours_col not in df.columns:
            logger.error(f"Missing required columns for expected delivery calculation")
            return df
        
        # Calculate expected delivery time
        df['expected_delivery_at_local'] = df.apply(
            lambda row: row[adjusted_start_col] + timedelta(hours=row[sla_hours_col])
            if pd.notna(row[adjusted_start_col]) and pd.notna(row[sla_hours_col])
            else pd.NaT,
            axis=1
        )
        
        # Convert to UTC
        df['expected_delivery_at_utc'] = df['expected_delivery_at_local'].apply(
            lambda x: x.astimezone(pd.Timestamp.utcnow().tz) if pd.notna(x) and x.tzinfo else pd.NaT
        )
        
        logger.info("Calculated expected delivery times")
        return df
    
    def determine_sla_status(self, df: pd.DataFrame,
                           delivered_at_col: str = 'delivered_at_local',
                           expected_delivery_col: str = 'expected_delivery_at_local',
                           order_status_col: str = 'order_status') -> pd.DataFrame:
        """Determine SLA status for each parcel
        
        Rules:
        - Delivered parcels: "On Time" if delivered <= expected, else "Late"
        - Non-delivered parcels: "Open"
        
        Args:
            df: DataFrame with delivery timestamps
            delivered_at_col: Column with actual delivery time
            expected_delivery_col: Column with expected delivery time
            order_status_col: Column with order status
        
        Returns:
            DataFrame with sla_status column
        """
        df = df.copy()
        
        df['sla_status'] = 'Open'
        
        # Check if parcel is delivered - convert to string first to handle any numeric values
        df[order_status_col] = df[order_status_col].astype(str)
        delivered_mask = df[order_status_col].str.lower().isin(['delivered', 'complete', 'completed'])
        
        # For delivered parcels, compare actual vs expected
        for idx in df[delivered_mask].index:
            delivered_at = df.at[idx, delivered_at_col]
            expected_at = df.at[idx, expected_delivery_col]
            
            if pd.isna(delivered_at) or pd.isna(expected_at):
                df.at[idx, 'sla_status'] = 'Unknown'
                continue
            
            if delivered_at <= expected_at:
                df.at[idx, 'sla_status'] = 'On Time'
            else:
                df.at[idx, 'sla_status'] = 'Late'
        
        # Log statistics
        status_counts = df['sla_status'].value_counts()
        logger.info(f"SLA status distribution: {status_counts.to_dict()}")
        
        return df
    
    def tag_late_reasons(self, df: pd.DataFrame,
                        freshdesk_df: Optional[pd.DataFrame] = None,
                        warehouse_col: str = 'warehouse',
                        order_time_col: str = 'order_created_at_local') -> pd.DataFrame:
        """Tag late reasons for parcels
        
        Tags applied:
        - after_cutoff: Order created after cutoff time
        - before_shift: Order created before shift start
        - unknown_zone: Zone mapping failed
        - missing_events: Required timestamps are null
        - delivery_delay, damaged_item, etc.: From FreshDesk data
        - exceeded_sla: Fallback for other late parcels
        
        Args:
            df: DataFrame with parcels
            freshdesk_df: Optional FreshDesk data with issue tags
            warehouse_col: Warehouse column name
            order_time_col: Order creation time column
        
        Returns:
            DataFrame with late_tags and late_primary_reason columns
        """
        df = df.copy()
        
        df['late_tags'] = ''
        df['late_primary_reason'] = None
        
        # Only process late parcels
        late_mask = df['sla_status'] == 'Late'
        
        for idx in df[late_mask].index:
            tags = []
            
            # Check for missing events
            required_timestamps = ['picked_at_local', 'packed_at_local', 
                                 'out_for_delivery_at_local', 'delivered_at_local']
            missing_count = sum(1 for col in required_timestamps 
                              if col in df.columns and pd.isna(df.at[idx, col]))
            if missing_count > 0:
                tags.append('missing_events')
            
            # Check for unknown zone
            if 'unknown_zone' in df.columns and df.at[idx, 'unknown_zone']:
                tags.append('unknown_zone')
            
            # Check for before shift / after cutoff
            warehouse = df.at[idx, warehouse_col] if warehouse_col in df.columns else None
            order_time = df.at[idx, order_time_col] if order_time_col in df.columns else None
            
            if warehouse and order_time and pd.notna(order_time) and warehouse in self.warehouse_configs:
                config = self.warehouse_configs[warehouse]
                is_ramadan = config.is_ramadan_period(order_time)
                shift_start = config.get_shift_start_time(is_ramadan)
                cutoff_time = config.get_cutoff_time(is_ramadan)
                
                order_time_only = order_time.time()
                
                if order_time_only < shift_start:
                    tags.append('before_shift')
                elif order_time_only > cutoff_time:
                    tags.append('after_cutoff')
            
            # Check FreshDesk issues
            if freshdesk_df is not None and not freshdesk_df.empty:
                parcel_id = df.at[idx, 'parcel_id'] if 'parcel_id' in df.columns else None
                if parcel_id:
                    freshdesk_issues = freshdesk_df[freshdesk_df['parcel_id'] == parcel_id]
                    for _, issue in freshdesk_issues.iterrows():
                        issue_type = issue.get('issue_type', '').lower()
                        if 'delay' in issue_type:
                            tags.append('delivery_delay')
                        elif 'damage' in issue_type:
                            tags.append('damaged_item')
                        elif 'unavailable' in issue_type or 'customer' in issue_type:
                            tags.append('customer_unavailable')
                        elif 'address' in issue_type:
                            tags.append('address_issue')
            
            # Fallback tag
            if not tags:
                tags.append('exceeded_sla')
            
            # Store tags
            df.at[idx, 'late_tags'] = '|'.join(tags)
            
            # Determine primary reason (highest priority)
            primary = max(tags, key=lambda t: self.LATE_REASON_PRIORITY.get(t, 0))
            df.at[idx, 'late_primary_reason'] = primary
        
        logger.info(f"Tagged late reasons for {late_mask.sum()} late parcels")
        return df
    
    def process_sla(self, df: pd.DataFrame,
                   freshdesk_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """Complete SLA processing pipeline
        
        Assumes df already has:
        - adjusted_start_time (from ShiftCutoffRules)
        - sla_hours (from LocationMapper)
        - delivered_at_local
        - order_status
        
        Args:
            df: DataFrame with required columns
            freshdesk_df: Optional FreshDesk data
        
        Returns:
            DataFrame with SLA status and late reasons
        """
        df = self.calculate_expected_delivery(df)
        df = self.determine_sla_status(df)
        df = self.tag_late_reasons(df, freshdesk_df)
        
        return df
