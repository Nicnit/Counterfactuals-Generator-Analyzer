"""Event management."""

import pandas as pd
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
from .utils import normalize_timezone


@dataclass
class Event:
    """Event for counterfactual analysis."""
    start: pd.Timestamp
    end: pd.Timestamp
    name: str
    metadata: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        """Validate event dates and normalize timezones."""
        self.start = normalize_timezone(self.start)
        self.end = normalize_timezone(self.end)
        
        if self.start >= self.end:
            raise ValueError(
                f"Invalid event dates for {self.name}: "
                f"start {self.start} >= end {self.end}"
            )
    
    def duration(self) -> pd.Timedelta:
        """Get event duration."""
        return self.end - self.start
    
    def contains(self, timestamp: pd.Timestamp) -> bool:
        """Check if timestamp is within event period (inclusive)."""
        timestamp = normalize_timezone(timestamp)
        return self.start <= timestamp <= self.end
    
    def overlaps(self, other: 'Event') -> bool:
        """Check if this event overlaps with another event."""
        return not (self.end < other.start or other.end < self.start)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary."""
        return {
            'start': self.start,
            'end': self.end,
            'name': self.name,
            'metadata': self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Event':
        """Create event from dictionary."""
        return cls(
            start=pd.Timestamp(data['start']),
            end=pd.Timestamp(data['end']),
            name=data['name'],
            metadata=data.get('metadata')
        )


class EventManager:
    """
    Manage multiple events and their relationships.
    """
    
    def __init__(self, events: List[Event]):
        """Initialize event manager."""
        self.events = events
        self._validate_events()
    
    def _validate_events(self):
        """Validate all events."""
        for event in self.events:
            event.__post_init__()  # Trigger validation
    
    def find_overlapping(self) -> List[tuple]:
        """Find overlapping events."""
        overlapping = []
        for i, event1 in enumerate(self.events):
            for event2 in self.events[i+1:]:
                if event1.overlaps(event2):
                    overlapping.append((event1, event2))
        return overlapping
    
    def filter_by_date_range(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp
    ) -> List[Event]:
        """Filter events within date range."""
        start = normalize_timezone(start)
        end = normalize_timezone(end)
        
        return [
            event for event in self.events
            if event.start <= end and event.end >= start
        ]
    
    def get_event_by_name(self, name: str) -> Optional[Event]:
        """Get event by name."""
        for event in self.events:
            if event.name == name:
                return event
        return None
    
    def to_list(self) -> List[Dict[str, Any]]:
        """Convert all events to list of dictionaries."""
        return [event.to_dict() for event in self.events]

