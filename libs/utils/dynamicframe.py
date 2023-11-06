from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame, DataFrame

import boto3


class DynamicFrameReaderWriter:

    glue_context: GlueContext
    connection: dict

    def __init__(self, glue_context: GlueContext, jdbc_connection_name: str = ""):
        self.glue_context = glue_context
        self.connection = {}

        if jdbc_connection_name:
            self.set_connection(jdbc_connection_name)

    def set_connection(self, jdbc_connection_name: str) -> None:
        # NOTE Require vpc endpoint for glue endpoint
        client = boto3.client("glue")
        self.connection = client.get_connection(Name=jdbc_connection_name)

    def read_dyf_from_s3(self, s3key: str, **kwargs) -> DynamicFrame:
        connection_options = {
            "paths": [s3key],
            "groupFiles": "none",
            "recurse": True,
        }

        for key, value in kwargs.items():
            connection_options[key] = value

        return self.glue_context.create_dynamic_frame_from_options("s3", connection_options, "parquet")

    def read_dyf_from_jdbc(self, database: str, table: str) -> DynamicFrame:
        url = self.connection["Connection"]["ConnectionProperties"]["JDBC_CONNECTION_URL"].split("/")
        url[-1] = database
        url = "/".join(url)
        connection_options = {
            "url": url,
            "user": self.connection["Connection"]["ConnectionProperties"]["USERNAME"],
            "password": self.connection["Connection"]["ConnectionProperties"]["PASSWORD"],
            "dbtable": table,
        }
        return self.glue_context.create_dynamic_frame_from_options("mysql", connection_options)

    def write_df_from_jdbc(self, dataframe: DataFrame | DynamicFrame, database: str, table: str) -> None:
        is_df = isinstance(dataframe, DataFrame)
        write_dyf = DynamicFrame.fromDF(dataframe, self.glue_context, f"{database}.{table}") if is_df else dataframe
        self.glue_context.write_dynamic_frame.from_jdbc_conf(
            frame=write_dyf,
            catalog_connection=self.connection["Connection"]["Name"],
            connection_options={
                "dbtable": table,
                "database": database,
            }
        )
