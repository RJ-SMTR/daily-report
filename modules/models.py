from typing import Any
from datetime import datetime
from dateutil import parser

from uuid import UUID
import pytz

class Schedule:
    flow_id: UUID
    start_date: datetime
    end_date: datetime
    interval: int

    def __init__(self, **kwargs: Any):
        self.flow_id = UUID(kwargs['flow_id'])
        if kwargs['start_date']:
            self.start_date = parser.parse(kwargs['start_date']['dt']).astimezone(
                pytz.timezone(kwargs['start_date']['tz']))
        else:
            self.start_date = kwargs['start_date']
        self.end_date = kwargs['end_date']
        if 'interval' in kwargs:
            self.interval = kwargs['interval']
        else:
            self.interval = None
