"""CLI entry point for Operations KPIs"""
import argparse
import logging
import sys
from pathlib import Path
import yaml
import pandas as pd

from src.ingest import DataIngester, DatasetLoader
from src.normalize import normalize_dataframe
from src.types import WarehouseConfig, RamadanConfig
from src.mapping import LocationMapper
from src.fact_table import FactTableBuilder
from src.metrics import MetricsGenerator
from src.quality import QualityValidator
from src.export import DataExporter

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_warehouse_configs(config_path: Path) -> dict:
    """Load warehouse configurations from YAML"""
    with open(config_path, 'r') as f:
        data = yaml.safe_load(f)
    
    configs = {}
    for name, config_data in data['warehouses'].items():
        ramadan_data = config_data.get('ramadan', {})
        ramadan_config = RamadanConfig(
            enabled=ramadan_data.get('enabled', False),
            date_ranges=ramadan_data.get('date_ranges', []),
            shift_start=ramadan_data.get('shift_start'),
            shift_end=ramadan_data.get('shift_end'),
            cutoff_time=ramadan_data.get('cutoff_time')
        )
        
        configs[name] = WarehouseConfig(
            name=name,
            timezone=config_data['timezone'],
            country_code=config_data['country_code'],
            shift_start=config_data['shift_start'],
            shift_end=config_data['shift_end'],
            cutoff_time=config_data['cutoff_time'],
            default_delivery_sla_hours=config_data['default_delivery_sla_hours'],
            ramadan=ramadan_config
        )
    
    return configs


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description='Operations KPIs - Parcel KPI Engine')
    parser.add_argument('--input', type=str, required=True, help='Path to input data folder')
    parser.add_argument('--config', type=str, default='./config', help='Path to config folder')
    parser.add_argument('--out', type=str, default='./out', help='Path to output folder')
    parser.add_argument('--from-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--to-date', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--warehouses', type=str, help='Comma-separated warehouse names')
    parser.add_argument('--timezone-default', type=str, default='Asia/Kuwait', help='Default timezone')
    parser.add_argument('--log-level', type=str, default='INFO', choices=['INFO', 'DEBUG'])
    
    args = parser.parse_args()
    
    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    try:
        # Setup paths
        input_path = Path(args.input)
        config_path = Path(args.config)
        output_path = Path(args.out)
        output_path.mkdir(parents=True, exist_ok=True)
        
        logger.info("=" * 60)
        logger.info("Operations KPIs - Starting Pipeline")
        logger.info("=" * 60)
        
        # Load configurations
        logger.info("Loading configurations...")
        warehouse_configs = load_warehouse_configs(config_path / 'warehouses.yaml')
        logger.info(f"Loaded {len(warehouse_configs)} warehouse configurations")
        
        # Initialize ingester
        schema_map_path = config_path / 'schema_map.yaml'
        ingester = DataIngester(schema_map_path if schema_map_path.exists() else None)
        loader = DatasetLoader(ingester)
        
        # Load datasets
        logger.info("Loading datasets...")
        datasets = {}
        
        dataset_files = {
            'delivery_details': 'Delivery Details - Sheet1.csv',
            'parcel_logs': 'Parcel Logs - Sheet1.csv',
            'collectors_report': 'Collectors Report - Sheet1.csv',
            'prepare_report': 'Prepare Report - Sheet1.csv',
            'items_per_order': 'Items Per Order Report - Sheet1.csv',
            'freshdesk_data': 'FreshDesk Data - Sheet1.csv'
        }
        
        for dataset_name, filename in dataset_files.items():
            file_path = input_path / filename
            if file_path.exists():
                try:
                    if dataset_name == 'delivery_details':
                        datasets[dataset_name] = loader.load_delivery_details(file_path)
                    elif dataset_name == 'parcel_logs':
                        datasets[dataset_name] = loader.load_parcel_logs(file_path)
                    elif dataset_name == 'collectors_report':
                        datasets[dataset_name] = loader.load_collectors_report(file_path)
                    elif dataset_name == 'prepare_report':
                        datasets[dataset_name] = loader.load_prepare_report(file_path)
                    elif dataset_name == 'items_per_order':
                        datasets[dataset_name] = loader.load_items_per_order(file_path)
                    elif dataset_name == 'freshdesk_data':
                        datasets[dataset_name] = loader.load_freshdesk_data(file_path)
                except Exception as e:
                    logger.error(f"Error loading {dataset_name}: {e}")
            else:
                logger.warning(f"File not found: {filename}")
        
        logger.info(f"Loaded {len(datasets)} datasets")
        
        # Normalize data
        logger.info("Normalizing data...")
        for name, df in datasets.items():
            datasets[name] = normalize_dataframe(df, detect_wa=True)
        
        # Load SLA configuration
        logger.info("Loading SLA configuration...")
        sla_config_path = config_path / 'sla_by_area.csv'
        sla_config_df = pd.DataFrame()
        if sla_config_path.exists():
            sla_config_df = pd.read_csv(sla_config_path)
            logger.info(f"Loaded SLA config with {len(sla_config_df)} entries")
        else:
            logger.warning("SLA config not found, using warehouse defaults only")
        
        # Initialize location mapper
        location_mapper = LocationMapper(sla_config_df, warehouse_configs)
        
        # Build fact table
        logger.info("Building fact table...")
        fact_builder = FactTableBuilder(warehouse_configs, location_mapper)
        fact, validation = fact_builder.build(
            delivery_details=datasets.get('delivery_details', pd.DataFrame()),
            parcel_logs=datasets.get('parcel_logs', pd.DataFrame()),
            collectors_report=datasets.get('collectors_report'),
            prepare_report=datasets.get('prepare_report'),
            items_per_order=datasets.get('items_per_order'),
            freshdesk_data=datasets.get('freshdesk_data')
        )
        logger.info(f"Built fact table with {len(fact)} rows")
        
        # Check for validation errors
        if validation['errors']:
            logger.error("Validation errors found:")
            for error in validation['errors']:
                logger.error(f"  - {error}")
            return 2
        
        if validation['warnings']:
            logger.warning("Validation warnings:")
            for warning in validation['warnings']:
                logger.warning(f"  - {warning}")
        
        # Generate metrics
        logger.info("Generating metrics...")
        metrics_gen = MetricsGenerator()
        reports = metrics_gen.generate_all_reports(fact)
        logger.info(f"Generated {len(reports)} reports")
        
        # Export outputs
        logger.info("Exporting outputs...")
        exporter = DataExporter(output_path)
        exported_files = exporter.export_all_reports(reports)
        
        # Export fact table (optional)
        fact_path = exporter.export_fact_table(fact, format='parquet')
        if fact_path:
            exported_files['fact_table'] = fact_path
        
        logger.info(f"Exported {len(exported_files)} files:")
        for name, path in exported_files.items():
            logger.info(f"  - {name}: {path}")
        
        # Generate quality report
        logger.info("Generating quality report...")
        quality_validator = QualityValidator(warehouse_configs)
        quality_report = quality_validator.run_all_validations(fact)
        quality_validator.export_quality_report(quality_report, output_path / 'quality_report.json')
        
        logger.info("=" * 60)
        logger.info("Pipeline completed successfully!")
        logger.info(f"Output saved to: {output_path}")
        logger.info("=" * 60)
        
        return 0
        
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        return 2
    except Exception as e:
        logger.error(f"Runtime error: {e}", exc_info=True)
        return 3


if __name__ == '__main__':
    sys.exit(main())
