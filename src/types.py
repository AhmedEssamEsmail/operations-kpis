"""Data types and configuration models for Operations KPIs"""
from dataclasses import dataclass, field
from datetime import datetime, time
from typing import Optional, List, Dict


@dataclass
class RamadanConfig:
    """Ramadan-specific configuration"""
    enabled: bool = False
    date_ranges: List[Dict[str, str]] = field(default_factory=list)
    shift_start: Optional[str] = None
    shift_end: Optional[str] = None
    cutoff_time: Optional[str] = None
    iftar_break_start: Optional[str] = None
    iftar_break_end: Optional[str] = None


@dataclass
class WarehouseConfig:
    """Warehouse configuration"""
    name: str
    timezone: str
    country_code: str
    shift_start: str
    shift_end: str
    cutoff_time: str
    default_delivery_sla_hours: float
    ramadan: Optional[RamadanConfig] = None
    
    def get_shift_start_time(self, is_ramadan: bool = False) -> time:
        """Get shift start time as time object"""
        if is_ramadan and self.ramadan and self.ramadan.shift_start:
            hour, minute = map(int, self.ramadan.shift_start.split(':'))
        else:
            hour, minute = map(int, self.shift_start.split(':'))
        return time(hour, minute)
    
    def get_shift_end_time(self, is_ramadan: bool = False) -> time:
        """Get shift end time as time object"""
        if is_ramadan and self.ramadan and self.ramadan.shift_end:
            hour, minute = map(int, self.ramadan.shift_end.split(':'))
        else:
            hour, minute = map(int, self.shift_end.split(':'))
        return time(hour, minute)
    
    def get_cutoff_time(self, is_ramadan: bool = False) -> time:
        """Get cutoff time as time object"""
        if is_ramadan and self.ramadan and self.ramadan.cutoff_time:
            hour, minute = map(int, self.ramadan.cutoff_time.split(':'))
        else:
            hour, minute = map(int, self.cutoff_time.split(':'))
        return time(hour, minute)
    
    def is_ramadan_period(self, check_date: datetime) -> bool:
        """Check if given date falls within Ramadan period"""
        if not self.ramadan or not self.ramadan.enabled:
            return False
        
        for date_range in self.ramadan.date_ranges:
            start = datetime.strptime(date_range['start'], '%Y-%m-%d').date()
            end = datetime.strptime(date_range['end'], '%Y-%m-%d').date()
            if start <= check_date.date() <= end:
                return True
        return False


@dataclass
class SLAConfig:
    """SLA configuration for a specific area"""
    warehouse: str
    city: Optional[str] = None
    area: Optional[str] = None
    zone: Optional[str] = None
    sla_hours: float = 4.0


@dataclass
class SchemaMapping:
    """Schema mapping configuration"""
    field_mappings: Dict[str, List[str]] = field(default_factory=dict)
    
    def get_standard_name(self, column_name: str) -> Optional[str]:
        """Get standard field name for a given column name"""
        column_lower = column_name.lower().strip()
        for standard_name, aliases in self.field_mappings.items():
            for alias in aliases:
                if alias.lower().strip() == column_lower:
                    return standard_name
        return None
    
    def map_columns(self, columns: List[str]) -> Dict[str, str]:
        """Map a list of columns to standard names"""
        mapping = {}
        for col in columns:
            standard = self.get_standard_name(col)
            if standard:
                mapping[col] = standard
        return mapping
