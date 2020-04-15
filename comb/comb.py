import pandas as pd
from pyhive import hive, presto


class Comb:
    def __init__(self, addr="localhost"):
        self.addr = addr

        self.query_fns = {
            "presto": self.presto_query,
            "hive": self.hive_query,
            "gbq": self.gbq_query
        }

        self.type_conversions = {

        }

        self.default_gbq_project = "places-clickstream"

    def run_query(self, query, type="presto"):
        """
        General wrapper function around querying with different engines
        """
        query_fn = self.query_fns[type]
        df = query_fn(query, self)
        return df

    def hive_query(self, query, convert_dtypes=True):
        """
        Hive-specific query function
        """
        with hive.connect("localhost") as conn:
            df = pd.read_sql(query, conn)
        if convert_dtypes:
            df = self.convert_dtypes(df)
        return df

    def presto_query(self, query, convert_dtypes=True):
        """
        Presto-specific query function
        """
        with presto.connect("localhost") as conn:
            df = pd.read_sql(query, conn)
        if convert_dtypes:
            df = self.convert_dtypes(df)
        return df

    def gbq_query(self, query, convert_dtypes=True):
        """
        BigQuery-specific query function
        """
        df = pd.read_gbq(query, project_id=self.default_gbq_project)
        if convert_dtypes:
            df = self.convert_dtypes(df)
        return df

    def convert_dtypes_dl(self, df, extra_conversions=None):
        """
        Converts all dtypes
        """
        type_conversions = self.type_conversions.update(extra_conversions)

        df_types = df.dtypes.to_frame()
        for old_type, new_type in type_conversions.items():
            df_types[df_types[0] == old_type] = new_type

        dict_types = df_types.to_dict()[0]
        df.astype(dict_types)

    # Won't be quite so easy
    # def convert_dtypes_ul(self, df, extra_conversions=None):
    #     type_conversions = self.type_conversions.update(extra_conversions)
    #     rev_type_conversions = {value: key for key, value
    #                             in type_conversions.items()}

    def describe_table(self, table_name):
        with presto.connect(self.addr) as conn:
            desc_query = "DESCRIBE EXTENDED {}".format(table_name)
            desc = pd.read_sql(desc_query, conn)
        return desc
