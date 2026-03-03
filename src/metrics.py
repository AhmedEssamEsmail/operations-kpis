"""Metrics and aggregation module"""
import pandas as pd
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class MetricsGenerator:
    """Generates aggregated metrics and reports from fact table"""
    
    def __init__(self):
        pass
    
    def generate_dod_daily(self, fact: pd.DataFrame) -> pd.DataFrame:
        """Generate Daily DOD (Delivered On Date) report
        
        Groups by warehouse and delivery date (local), calculates:
        - delivered: Count of delivered parcels
        - on_time: Count of on-time parcels
        - late: Count of late parcels
        - on_time_pct: Percentage on time
        
        Args:
            fact: Fact table with SLA status
        
        Returns:
            DataFrame with daily DOD metrics
        """
        if fact.empty:
            return pd.DataFrame()
        
        # Filter to delivered parcels only
        delivered_mask = fact['sla_status'].isin(['On Time', 'Late'])
        delivered = fact[delivered_mask].copy()
        
        if delivered.empty:
            logger.warning("No delivered parcels found for DOD report")
            return pd.DataFrame()
        
        # Extract date from delivered_at_local
        if 'delivered_at_local' not in delivered.columns:
            logger.error("delivered_at_local column not found")
            return pd.DataFrame()
        
        delivered['delivery_date'] = pd.to_datetime(delivered['delivered_at_local']).dt.date
        
        # Group by warehouse and date
        dod = delivered.groupby(['warehouse', 'delivery_date']).agg({
            'parcel_id': 'count',
            'sla_status': lambda x: (x == 'On Time').sum()
        }).reset_index()
        
        dod.columns = ['warehouse', 'delivery_date', 'delivered', 'on_time']
        dod['late'] = dod['delivered'] - dod['on_time']
        
        # Calculate on_time_pct (guard against zero division)
        dod['on_time_pct'] = (dod['on_time'] / dod['delivered'] * 100).round(2)
        dod['on_time_pct'] = dod['on_time_pct'].fillna(0)
        
        # Sort by date and warehouse
        dod = dod.sort_values(['delivery_date', 'warehouse'])
        
        logger.info(f"Generated DOD daily report: {len(dod)} rows")
        return dod
    
    def generate_sla_breakdown(self, fact: pd.DataFrame) -> pd.DataFrame:
        """Generate SLA breakdown report by location
        
        Groups by warehouse, zone, city, area, calculates:
        - delivered: Count of delivered parcels
        - on_time: Count of on-time parcels
        - late: Count of late parcels
        - on_time_pct: Percentage on time
        
        Args:
            fact: Fact table with SLA status and location data
        
        Returns:
            DataFrame with SLA breakdown by location
        """
        if fact.empty:
            return pd.DataFrame()
        
        # Filter to delivered parcels only
        delivered_mask = fact['sla_status'].isin(['On Time', 'Late'])
        delivered = fact[delivered_mask].copy()
        
        if delivered.empty:
            logger.warning("No delivered parcels found for SLA breakdown")
            return pd.DataFrame()
        
        # Group by location dimensions
        group_cols = ['warehouse']
        if 'zone' in delivered.columns:
            group_cols.append('zone')
        if 'city' in delivered.columns:
            group_cols.append('city')
        if 'area' in delivered.columns:
            group_cols.append('area')
        
        sla_breakdown = delivered.groupby(group_cols).agg({
            'parcel_id': 'count',
            'sla_status': lambda x: (x == 'On Time').sum()
        }).reset_index()
        
        sla_breakdown.columns = group_cols + ['delivered', 'on_time']
        sla_breakdown['late'] = sla_breakdown['delivered'] - sla_breakdown['on_time']
        
        # Calculate on_time_pct
        sla_breakdown['on_time_pct'] = (
            sla_breakdown['on_time'] / sla_breakdown['delivered'] * 100
        ).round(2)
        sla_breakdown['on_time_pct'] = sla_breakdown['on_time_pct'].fillna(0)
        
        # Sort by warehouse and volume
        sla_breakdown = sla_breakdown.sort_values(['warehouse', 'delivered'], ascending=[True, False])
        
        logger.info(f"Generated SLA breakdown report: {len(sla_breakdown)} rows")
        return sla_breakdown
    
    def generate_staff_productivity(self, fact: pd.DataFrame) -> pd.DataFrame:
        """Generate staff productivity report
        
        Groups by warehouse and staff member (collector/preparer/driver), calculates:
        - volume: Count of parcels handled
        - on_time: Count of on-time parcels
        - on_time_pct: Percentage on time
        
        Args:
            fact: Fact table with staff assignments
        
        Returns:
            DataFrame with staff productivity metrics
        """
        if fact.empty:
            return pd.DataFrame()
        
        staff_reports = []
        
        # Collectors productivity
        if 'collector_name' in fact.columns:
            collectors = fact[fact['collector_name'].notna()].copy()
            if not collectors.empty:
                collector_stats = collectors.groupby(['warehouse', 'collector_name']).agg({
                    'parcel_id': 'count',
                    'sla_status': lambda x: (x == 'On Time').sum()
                }).reset_index()
                collector_stats.columns = ['warehouse', 'staff_name', 'volume', 'on_time']
                collector_stats['role'] = 'Collector'
                staff_reports.append(collector_stats)
        
        # Preparers productivity
        if 'preparer_name' in fact.columns:
            preparers = fact[fact['preparer_name'].notna()].copy()
            if not preparers.empty:
                preparer_stats = preparers.groupby(['warehouse', 'preparer_name']).agg({
                    'parcel_id': 'count',
                    'sla_status': lambda x: (x == 'On Time').sum()
                }).reset_index()
                preparer_stats.columns = ['warehouse', 'staff_name', 'volume', 'on_time']
                preparer_stats['role'] = 'Preparer'
                staff_reports.append(preparer_stats)
        
        # Drivers productivity (if driver column exists)
        if 'driver_name' in fact.columns:
            drivers = fact[fact['driver_name'].notna()].copy()
            if not drivers.empty:
                driver_stats = drivers.groupby(['warehouse', 'driver_name']).agg({
                    'parcel_id': 'count',
                    'sla_status': lambda x: (x == 'On Time').sum()
                }).reset_index()
                driver_stats.columns = ['warehouse', 'staff_name', 'volume', 'on_time']
                driver_stats['role'] = 'Driver'
                staff_reports.append(driver_stats)
        
        if not staff_reports:
            logger.warning("No staff data found for productivity report")
            return pd.DataFrame()
        
        # Combine all staff reports
        staff_summary = pd.concat(staff_reports, ignore_index=True)
        
        # Calculate on_time_pct
        staff_summary['on_time_pct'] = (
            staff_summary['on_time'] / staff_summary['volume'] * 100
        ).round(2)
        staff_summary['on_time_pct'] = staff_summary['on_time_pct'].fillna(0)
        
        # Sort by role, warehouse, and volume
        staff_summary = staff_summary.sort_values(['role', 'warehouse', 'volume'], 
                                                  ascending=[True, True, False])
        
        logger.info(f"Generated staff productivity report: {len(staff_summary)} rows")
        return staff_summary
    
    def generate_all_reports(self, fact: pd.DataFrame) -> dict:
        """Generate all reports
        
        Args:
            fact: Fact table
        
        Returns:
            Dictionary with all report DataFrames
        """
        reports = {
            'dod_daily': self.generate_dod_daily(fact),
            'sla_breakdown': self.generate_sla_breakdown(fact),
            'staff_summary': self.generate_staff_productivity(fact)
        }
        
        logger.info("Generated all reports")
        return reports
