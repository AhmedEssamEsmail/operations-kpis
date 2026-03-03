"""Export module for CSV and Parquet outputs"""
import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class DataExporter:
    """Exports data to various formats"""
    
    def __init__(self, output_dir: str):
        """Initialize with output directory
        
        Args:
            output_dir: Path to output directory
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def clean_for_export(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean DataFrame for export
        
        Removes Excel error tokens like #N/A, #REF!, etc.
        
        Args:
            df: DataFrame to clean
        
        Returns:
            Cleaned DataFrame
        """
        df = df.copy()
        
        # Replace Excel error tokens with empty string
        error_tokens = ['#N/A', '#REF!', '#DIV/0!', '#VALUE!', '#NAME?', '#NUM!', '#NULL!']
        
        for col in df.columns:
            if df[col].dtype == 'object':
                for token in error_tokens:
                    df[col] = df[col].replace(token, '')
        
        return df
    
    def export_csv(self, df: pd.DataFrame, filename: str) -> str:
        """Export DataFrame to CSV
        
        Args:
            df: DataFrame to export
            filename: Output filename (without path)
        
        Returns:
            Full path to exported file
        """
        if df.empty:
            logger.warning(f"DataFrame is empty, skipping export of {filename}")
            return None
        
        # Clean data
        df = self.clean_for_export(df)
        
        # Export
        output_path = self.output_dir / filename
        df.to_csv(output_path, index=False)
        
        logger.info(f"Exported {len(df)} rows to {output_path}")
        return str(output_path)
    
    def export_parquet(self, df: pd.DataFrame, filename: str) -> str:
        """Export DataFrame to Parquet
        
        Args:
            df: DataFrame to export
            filename: Output filename (without path)
        
        Returns:
            Full path to exported file
        """
        if df.empty:
            logger.warning(f"DataFrame is empty, skipping export of {filename}")
            return None
        
        # Export
        output_path = self.output_dir / filename
        df.to_parquet(output_path, index=False, engine='pyarrow')
        
        logger.info(f"Exported {len(df)} rows to {output_path}")
        return str(output_path)
    
    def export_all_reports(self, reports: dict) -> dict:
        """Export all reports to CSV
        
        Args:
            reports: Dictionary of report DataFrames
                    Keys: 'dod_daily', 'sla_breakdown', 'staff_summary'
        
        Returns:
            Dictionary of exported file paths
        """
        exported_files = {}
        
        # Export DOD daily
        if 'dod_daily' in reports and not reports['dod_daily'].empty:
            path = self.export_csv(reports['dod_daily'], 'dod_daily.csv')
            if path:
                exported_files['dod_daily'] = path
        
        # Export SLA breakdown
        if 'sla_breakdown' in reports and not reports['sla_breakdown'].empty:
            path = self.export_csv(reports['sla_breakdown'], 'sla_breakdown.csv')
            if path:
                exported_files['sla_breakdown'] = path
        
        # Export staff summary
        if 'staff_summary' in reports and not reports['staff_summary'].empty:
            path = self.export_csv(reports['staff_summary'], 'staff_summary.csv')
            if path:
                exported_files['staff_summary'] = path
        
        logger.info(f"Exported {len(exported_files)} reports")
        return exported_files
    
    def export_fact_table(self, fact: pd.DataFrame, format: str = 'parquet') -> str:
        """Export fact table for fast re-runs
        
        Args:
            fact: Fact table DataFrame
            format: 'parquet' or 'csv'
        
        Returns:
            Path to exported file
        """
        if fact.empty:
            logger.warning("Fact table is empty, skipping export")
            return None
        
        if format == 'parquet':
            return self.export_parquet(fact, 'parcel_fact.parquet')
        else:
            return self.export_csv(fact, 'parcel_fact.csv')
