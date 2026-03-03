"""Location mapping and SLA lookup module"""
import pandas as pd
from typing import Dict, Optional
import logging

from src.types import WarehouseConfig

logger = logging.getLogger(__name__)


class LocationMapper:
    """Maps locations to zones and looks up SLA hours"""
    
    def __init__(self, 
                 sla_config_df: pd.DataFrame,
                 warehouse_configs: Dict[str, WarehouseConfig]):
        """Initialize with SLA configuration and warehouse configs
        
        Args:
            sla_config_df: DataFrame from sla_by_area.csv with columns:
                          warehouse, city, area, zone, sla_hours
            warehouse_configs: Dictionary of warehouse configurations
        """
        self.sla_config_df = sla_config_df
        self.warehouse_configs = warehouse_configs
        
        # Normalize for case-insensitive matching
        if not sla_config_df.empty:
            # Convert to string first to handle any numeric values
            self.sla_config_df['warehouse'] = self.sla_config_df['warehouse'].astype(str).str.strip().str.title()
            self.sla_config_df['city'] = self.sla_config_df['city'].astype(str).str.strip().str.title()
            self.sla_config_df['area'] = self.sla_config_df['area'].astype(str).str.strip().str.title()
    
    def map_zone(self, warehouse: str, city: str, area: Optional[str] = None) -> Optional[str]:
        """Map location to zone
        
        Lookup priority:
        1. (warehouse, city, area) - exact match
        2. (warehouse, city) - fallback if area not found
        3. None - if no match found
        
        Args:
            warehouse: Warehouse name
            city: City name
            area: Area name (optional)
        
        Returns:
            Zone name or None if not found
        """
        if self.sla_config_df.empty:
            return None
        
        # Normalize inputs
        warehouse = warehouse.strip().title() if warehouse else None
        city = city.strip().title() if city else None
        area = area.strip().title() if area else None
        
        # Try exact match with area
        if area:
            mask = (
                (self.sla_config_df['warehouse'] == warehouse) &
                (self.sla_config_df['city'] == city) &
                (self.sla_config_df['area'] == area)
            )
            matches = self.sla_config_df[mask]
            if not matches.empty:
                return matches.iloc[0]['zone']
        
        # Fallback to city-level match
        mask = (
            (self.sla_config_df['warehouse'] == warehouse) &
            (self.sla_config_df['city'] == city)
        )
        matches = self.sla_config_df[mask]
        if not matches.empty:
            return matches.iloc[0]['zone']
        
        return None
    
    def lookup_sla_hours(self, warehouse: str, city: str, area: Optional[str] = None) -> float:
        """Lookup SLA hours for a location
        
        Lookup priority:
        1. (warehouse, city, area) override - if exists in sla_by_area.csv
        2. (warehouse, city) override - if exists
        3. Warehouse default_delivery_sla_hours - fallback
        
        Args:
            warehouse: Warehouse name
            city: City name
            area: Area name (optional)
        
        Returns:
            SLA hours (float)
        """
        # Normalize inputs
        warehouse = warehouse.strip().title() if warehouse else None
        city = city.strip().title() if city else None
        area = area.strip().title() if area else None
        
        # Try exact match with area
        if area and not self.sla_config_df.empty:
            mask = (
                (self.sla_config_df['warehouse'] == warehouse) &
                (self.sla_config_df['city'] == city) &
                (self.sla_config_df['area'] == area)
            )
            matches = self.sla_config_df[mask]
            if not matches.empty and pd.notna(matches.iloc[0]['sla_hours']):
                return float(matches.iloc[0]['sla_hours'])
        
        # Fallback to city-level match
        if not self.sla_config_df.empty:
            mask = (
                (self.sla_config_df['warehouse'] == warehouse) &
                (self.sla_config_df['city'] == city)
            )
            matches = self.sla_config_df[mask]
            if not matches.empty and pd.notna(matches.iloc[0]['sla_hours']):
                return float(matches.iloc[0]['sla_hours'])
        
        # Fallback to warehouse default
        if warehouse in self.warehouse_configs:
            return self.warehouse_configs[warehouse].default_delivery_sla_hours
        
        # Ultimate fallback
        logger.warning(f"No SLA hours found for {warehouse}/{city}/{area}, using 24h default")
        return 24.0
    
    def enrich_dataframe(self, df: pd.DataFrame,
                        warehouse_col: str = 'warehouse',
                        city_col: str = 'city',
                        area_col: str = 'area') -> pd.DataFrame:
        """Enrich DataFrame with zone and SLA hours
        
        Adds columns:
        - zone: Mapped zone name
        - sla_hours: Resolved SLA hours
        - unknown_zone: Flag for unmapped locations
        
        Args:
            df: DataFrame with location columns
            warehouse_col: Warehouse column name
            city_col: City column name
            area_col: Area column name
        
        Returns:
            Enriched DataFrame
        """
        df = df.copy()
        
        # Initialize new columns
        df['zone'] = None
        df['sla_hours'] = None
        df['unknown_zone'] = False
        
        for idx in df.index:
            warehouse = df.at[idx, warehouse_col] if warehouse_col in df.columns else None
            city = df.at[idx, city_col] if city_col in df.columns else None
            area = df.at[idx, area_col] if area_col in df.columns else None
            
            if pd.isna(warehouse) or pd.isna(city):
                df.at[idx, 'unknown_zone'] = True
                continue
            
            # Map zone
            zone = self.map_zone(warehouse, city, area)
            df.at[idx, 'zone'] = zone
            
            if zone is None:
                df.at[idx, 'unknown_zone'] = True
            
            # Lookup SLA hours
            sla_hours = self.lookup_sla_hours(warehouse, city, area)
            df.at[idx, 'sla_hours'] = sla_hours
        
        # Log mapping statistics
        total = len(df)
        unknown = df['unknown_zone'].sum()
        logger.info(f"Zone mapping: {total - unknown}/{total} mapped ({unknown} unknown)")
        
        return df
