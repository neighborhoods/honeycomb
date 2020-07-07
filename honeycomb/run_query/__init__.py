from .bigquery import gbq_query
from .datalake import lake_query

__all__ = [
    'gbq_query',
    'lake_query'
]
