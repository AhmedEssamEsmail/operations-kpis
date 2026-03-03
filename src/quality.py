"""Quality validation module"""
import pandas as pd
import json
from typing import Dict, List
from pathlib import Path
import logging

from src.types import WarehouseConfig

logger = logging.getLogger(__name__)


class QualityValidator:
    """Validates data quality and generates quality reports"""
    
    def __init__(self, warehouse_configs: Dict[str, WarehouseConfig]):
        """Initialize with warehouse configurations"""
        self.warehouse_configs = warehouse_configs
    
    def validate_parcel_uniqueness(self, fact: pd.DataFrame) -> Dict:
        """Check parcel_id uniqueness
        
        Args:
            fact: Fact table
        
        Returns:
            Validation result dictionary
        """
        result = {
            'check': 'parcel_uniqueness',
            'passed': True,
            'errors': [],
            'metrics': {}
        }
        
        if 'parcel_id' not in fact.columns:
            result['passed'] = False
            result['errors'].append("parcel_id column not found")
            return result
        
        total = len(fact)
        unique = fact['parcel_id'].nunique()
        duplicates = total - unique
        
        result['metrics']['total_rows'] = total
        result['metrics']['unique_parcels'] = unique
        result['metrics']['duplicate_count'] = duplicates
        
        if duplicates > 0:
            result['passed'] = False
            result['errors'].append(f"{duplicates} duplicate parcel_ids found")
        
        return result
    
    def validate_required_columns(self, fact: pd.DataFrame) -> Dict:
        """Check for missing required columns
        
        Args:
            fact: Fact table
        
        Returns:
            Validation result dictionary
        """
        result = {
            'check': 'required_columns',
            'passed': True,
            'errors': [],
            'metrics': {}
        }
        
        required_cols = [
            'parcel_id', 'warehouse', 'order_status',
            'order_created_at', 'city'
        ]
        
        missing_cols = [col for col in required_cols if col not in fact.columns]
        
        result['metrics']['required_columns'] = required_cols
        result['metrics']['missing_columns'] = missing_cols
        
        if missing_cols:
            result['passed'] = False
            result['errors'].append(f"Missing required columns: {missing_cols}")
        
        return result
    
    def validate_delivered_timestamps(self, fact: pd.DataFrame) -> Dict:
        """Check for delivered parcels missing delivered_at
        
        Args:
            fact: Fact table
        
        Returns:
            Validation result dictionary
        """
        result = {
            'check': 'delivered_timestamps',
            'passed': True,
            'errors': [],
            'warnings': [],
            'metrics': {}
        }
        
        if 'order_status' not in fact.columns or 'delivered_at_local' not in fact.columns:
            result['passed'] = False
            result['errors'].append("Required columns not found")
            return result
        
        delivered_mask = fact['order_status'].str.lower().isin(['delivered', 'complete', 'completed'])
        delivered_count = delivered_mask.sum()
        missing_timestamp = fact[delivered_mask & fact['delivered_at_local'].isna()]
        missing_count = len(missing_timestamp)
        
        result['metrics']['delivered_parcels'] = int(delivered_count)
        result['metrics']['missing_timestamp'] = int(missing_count)
        
        if missing_count > 0:
            pct = (missing_count / delivered_count * 100) if delivered_count > 0 else 0
            result['warnings'].append(
                f"{missing_count} delivered parcels ({pct:.1f}%) missing delivered_at"
            )
        
        return result
    
    def validate_zone_mapping(self, fact: pd.DataFrame) -> Dict:
        """Calculate % unknown city/area/zone per warehouse
        
        Args:
            fact: Fact table
        
        Returns:
            Validation result dictionary
        """
        result = {
            'check': 'zone_mapping',
            'passed': True,
            'warnings': [],
            'metrics': {}
        }
        
        if 'warehouse' not in fact.columns:
            result['passed'] = False
            result['warnings'].append("warehouse column not found")
            return result
        
        # Calculate unknown zone percentage per warehouse
        if 'unknown_zone' in fact.columns:
            zone_stats = fact.groupby('warehouse').agg({
                'unknown_zone': ['sum', 'count']
            })
            zone_stats.columns = ['unknown', 'total']
            zone_stats['unknown_pct'] = (zone_stats['unknown'] / zone_stats['total'] * 100).round(2)
            
            result['metrics']['by_warehouse'] = {}
            for warehouse, stats in zone_stats.iterrows():
                result['metrics']['by_warehouse'][warehouse] = {
                    'total': int(stats['total']),
                    'unknown': int(stats['unknown']),
                    'unknown_pct': float(stats['unknown_pct'])
                }
                
                # Warn if >20% unknown
                if stats['unknown_pct'] > 20:
                    result['warnings'].append(
                        f"{warehouse}: {stats['unknown_pct']}% unknown zones"
                    )
        
        return result
    
    def validate_config_completeness(self) -> Dict:
        """Check config completeness
        
        Returns:
            Validation result dictionary
        """
        result = {
            'check': 'config_completeness',
            'passed': True,
            'errors': [],
            'metrics': {}
        }
        
        # Check warehouse configs
        if not self.warehouse_configs:
            result['passed'] = False
            result['errors'].append("No warehouse configurations found")
            return result
        
        result['metrics']['warehouse_count'] = len(self.warehouse_configs)
        result['metrics']['warehouses'] = list(self.warehouse_configs.keys())
        
        # Check each warehouse has required fields
        incomplete_warehouses = []
        for name, config in self.warehouse_configs.items():
            if not config.timezone:
                incomplete_warehouses.append(f"{name}: missing timezone")
            if not config.default_delivery_sla_hours:
                incomplete_warehouses.append(f"{name}: missing default_delivery_sla_hours")
        
        if incomplete_warehouses:
            result['passed'] = False
            result['errors'].extend(incomplete_warehouses)
        
        return result
    
    def run_all_validations(self, fact: pd.DataFrame) -> Dict:
        """Run all validation checks
        
        Args:
            fact: Fact table
        
        Returns:
            Complete validation report
        """
        report = {
            'timestamp': pd.Timestamp.now().isoformat(),
            'total_rows': len(fact),
            'validations': [],
            'summary': {
                'total_checks': 0,
                'passed': 0,
                'failed': 0,
                'warnings': 0
            }
        }
        
        # Run all validations
        validations = [
            self.validate_parcel_uniqueness(fact),
            self.validate_required_columns(fact),
            self.validate_delivered_timestamps(fact),
            self.validate_zone_mapping(fact),
            self.validate_config_completeness()
        ]
        
        report['validations'] = validations
        
        # Calculate summary
        report['summary']['total_checks'] = len(validations)
        for v in validations:
            if v['passed']:
                report['summary']['passed'] += 1
            else:
                report['summary']['failed'] += 1
            
            if 'warnings' in v and v['warnings']:
                report['summary']['warnings'] += len(v['warnings'])
        
        logger.info(f"Validation complete: {report['summary']['passed']}/{report['summary']['total_checks']} passed, "
                   f"{report['summary']['warnings']} warnings")
        
        return report
    
    def export_quality_report(self, report: Dict, output_path: str) -> None:
        """Export quality report to JSON file
        
        Args:
            report: Quality report dictionary
            output_path: Path to output JSON file
        """
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"Quality report exported to {output_path}")
