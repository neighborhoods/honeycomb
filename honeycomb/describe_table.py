import pandas as pd
from pyhive import hive, presto


def describe_table(self, table_name):
    with presto.connect(self.addr) as conn:
        desc_query = "DESCRIBE EXTENDED {}".format(table_name)
        desc = pd.read_sql(desc_query, conn)
    return desc
