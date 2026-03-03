"""Fact table builder module"""
import pandas as pd
from typing import Dict, Optional
import logging

from src.types import WarehouseConfig
from src.normalize import normalize_dataframe
from src.time_rules import TimestampExtractor, TimezoneHandler, ShiftCutoffRules
from src.mapping import LocationMapper
from src.sla_engine import SLAEngine

logger = logging.getLogger(__name__)


class FactTableBuilder:
    """Builds the parcel fact table by merging and enriching all datasets"""
    
    def __init__(self, warehouse_configs: Dict[str, WarehouseConfig],
                 location_mapper: LocationMapper):
        """Initialize with configurations
        
        Args:
            warehouse_configs: Dictionary of warehouse configurations
            location_mapper: LocationMapper instance for zone/SLA lookup
        """
        self.warehouse_configs = warehouse_configs
        self.location_mapper = location_mapper
        self.timestamp_extractor = TimestampExtractor()
        self.timezone_handler = TimezoneHandler(warehouse_configs)
        self.shift_cutoff_rules = ShiftCutoffRules(warehouse_configs)
        self.sla_engine = SLAEngine(warehouse_configs)
    
    def merge_datasets(self,
                      delivery_details: pd.DataFrame,
                      parcel_logs: pd.DataFrame,
                      collectors_report: Optional[pd.DataFrame] = None,
                      prepare_report: Optional[pd.DataFrame] = None,
                      items_per_order: Optional[pd.DataFrame] = None,
                      freshdesk_data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """Merge all datasets by parcel_id
        
        Args:
            delivery_details: Orders with delivery dates
            parcel_logs: Status transitions with timestamps
            collectors_report: Collector productivity data (optional)
            prepare_report: Preparer productivity data (optional)
            items_per_order: Item counts per order (optional)
            freshdesk_data: Issue/ticket data (optional)
        
        Returns:
            Merged DataFrame
        """
        if delivery_details.empty:
            logger.error("Delivery details is empty, cannot build fact table")
            return pd.DataFrame()
        
        # Start with delivery details as base
        fact = delivery_details.copy()
        logger.info(f"Starting with {len(fact)} orders from delivery details")
        
        # Extract timestamps from parcel logs
        if not parcel_logs.empty:
            timestamps = self.timestamp_extractor.extract_timestamps(parcel_logs)
            fact = fact.merge(timestamps, on='parcel_id', how='left')
            logger.info(f"Merged timestamps from parcel logs")
        
        # Merge collectors report
        if collectors_report is not None and not collectors_report.empty:
            # Assuming collectors_report has: parcel_id, collector_name, collect_time
            collectors_agg = collectors_report.groupby('parcel_id').agg({
                'collector_name': 'first',
                'collect_time': 'sum'
            }).reset_index()
            fact = fact.merge(collectors_agg, on='parcel_id', how='left')
            logger.info(f"Merged collectors report")
        
        # Merge prepare report
        if prepare_report is not None and not prepare_report.empty:
            # Assuming prepare_report has: parcel_id, preparer_name, prepare_time
            preparers_agg = prepare_report.groupby('parcel_id').agg({
                'preparer_name': 'first',
                'prepare_time': 'sum'
            }).reset_index()
            fact = fact.merge(preparers_agg, on='parcel_id', how='left')
            logger.info(f"Merged prepare report")
        
        # Merge items per order
        if items_per_order is not None and not items_per_order.empty:
            # Assuming items_per_order has: parcel_id, item_count
            items_agg = items_per_order.groupby('parcel_id').agg({
                'item_count': 'sum'
            }).reset_index()
            fact = fact.merge(items_agg, on='parcel_id', how='left')
            logger.info(f"Merged items per order")
        
        # Store freshdesk data separately for late reason tagging
        self.freshdesk_data = freshdesk_data
        
        logger.info(f"Merged all datasets: {len(fact)} rows")
        return fact
    
    def enrich_fact_table(self, fact: pd.DataFrame) -> pd.DataFrame:
        """Apply all enrichment steps to fact table
        
        Steps:
        1. Normalize and clean data
        2. Extract and localize timestamps
        3. Calculate phase durations
        4. Map zones and SLA hours
        5. Calculate expected delivery times
        6. Determine SLA status and late reasons
        
        Args:
            fact: Merged fact table
        
        Returns:
            Enriched fact table
        """
        if fact.empty:
            return fact
        
        logger.info("Starting fact table enrichment")
        
        # 1. Normalize data
        fact = normalize_dataframe(fact)
        logger.info("Applied normalization")
        
        # 2. Localize timestamps
        timestamp_cols = ['order_created_at', 'picked_at', 'packed_at', 
                         'out_for_delivery_at', 'delivered_at']
        existing_timestamp_cols = [col for col in timestamp_cols if col in fact.columns]
        
        if existing_timestamp_cols:
            fact = self.timezone_handler.localize_timestamps(fact, existing_timestamp_cols)
            logger.info("Localized timestamps")
        
        # 3. Calculate phase durations
        fact = self.timezone_handler.calculate_durations(fact)
        logger.info("Calculated phase durations")
        
        # 4. Map zones and SLA hours
        fact = self.location_mapper.enrich_dataframe(fact)
        logger.info("Mapped zones and SLA hours")
        
        # 5. Calculate adjusted start time
        if 'order_created_at_local' in fact.columns:
            fact = self.shift_cutoff_rules.calculate_adjusted_start_time(fact)
            logger.info("Calculated adjusted start times")
        
        # 6. Process SLA
        fact = self.sla_engine.process_sla(fact, self.freshdesk_data)
        logger.info("Processed SLA status and late reasons")
        
        logger.info("Fact table enrichment complete")
        return fact
    
    def validate_fact_table(self, fact: pd.DataFrame) -> Dict:
        """Validate fact table and generate quality metrics
        
        Checks:
        - Parcel ID uniqueness
        - Missing required columns
        - Delivered parcels missing delivered_at
        - Unknown zone percentage per warehouse
        
        Args:
            fact: Fact table to validate
        
        Returns:
            Dictionary with validation results
        """
        validation = {
            'total_rows': len(fact),
            'errors': [],
            'warnings': [],
            'metrics': {}
        }
        
        if fact.empty:
            validation['errors'].append("Fact table is empty")
            return validation
        
        # Check parcel_id uniqueness
        if 'parcel_id' in fact.columns:
            duplicate_count = fact['parcel_id'].duplicated().sum()
            if duplicate_count > 0:
                validation['errors'].append(f"{duplicate_count} duplicate parcel_ids found")
            validation['metrics']['unique_parcels'] = fact['parcel_id'].nunique()
        
        # Check for missing required columns
        required_cols = ['parcel_id', 'warehouse', 'order_status']
        missing_cols = [col for col in required_cols if col not in fact.columns]
        if missing_cols:
            validation['errors'].append(f"Missing required columns: {missing_cols}")
        
        # Check delivered parcels missing delivered_at
        if 'order_status' in fact.columns and 'delivered_at_local' in fact.columns:
            delivered_mask = fact['order_status'].str.lower().isin(['delivered', 'complete'])
            missing_delivery_time = fact[delivered_mask & fact['delivered_at_local'].isna()]
            if len(missing_delivery_time) > 0:
                validation['warnings'].append(
                    f"{len(missing_delivery_time)} delivered parcels missing delivered_at"
                )
        
        # Calculate unknown zone percentage per warehouse
        if 'warehouse' in fact.columns and 'unknown_zone' in fact.columns:
            zone_health = fact.groupby('warehouse').agg({
                'unknown_zone': ['sum', 'count']
            })
            zone_health.columns = ['unknown', 'total']
            zone_health['unknown_pct'] = (zone_health['unknown'] / zone_health['total'] * 100).round(2)
            validation['metrics']['zone_mapping_health'] = zone_health.to_dict('index')
            
            # Warn if any warehouse has >20% unknown zones
            for warehouse, stats in zone_health.iterrows():
                if stats['unknown_pct'] > 20:
                    validation['warnings'].append(
                        f"{warehouse}: {stats['unknown_pct']}% unknown zones"
                    )
        
        # SLA status distribution
        if 'sla_status' in fact.columns:
            sla_dist = fact['sla_status'].value_counts().to_dict()
            validation['metrics']['sla_status_distribution'] = sla_dist
        
        logger.info(f"Validation complete: {len(validation['errors'])} errors, "
                   f"{len(validation['warnings'])} warnings")
        
        return validation
    
    def build(self,
             delivery_details: pd.DataFrame,
             parcel_logs: pd.DataFrame,
             collectors_report: Optional[pd.DataFrame] = None,
             prepare_report: Optional[pd.DataFrame] = None,
             items_per_order: Optional[pd.DataFrame] = None,
             freshdesk_data: Optional[pd.DataFrame] = None) -> tuple[pd.DataFrame, Dict]:
        """Complete fact table build pipeline
        
        Args:
            delivery_details: Orders with delivery dates
            parcel_logs: Status transitions
            collectors_report: Collector data (optional)
            prepare_report: Preparer data (optional)
            items_per_order: Item counts (optional)
            freshdesk_data: Issue data (optional)
        
        Returns:
            Tuple of (fact_table, validation_results)
        """
        # Merge datasets
        fact = self.merge_datasets(
            delivery_details, parcel_logs, collectors_report,
            prepare_report, items_per_order, freshdesk_data
        )
        
        # Enrich
        fact = self.enrich_fact_table(fact)
        
        # Validate
        validation = self.validate_fact_table(fact)
        
        return fact, validation
