from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame, DataFrame

from utils.jdbc import JDBCConnectionContainer

class DynamicFrameReaderWriter(JDBCConnectionContainer):

    glue_context: GlueContext

    def __init__(self, glue_context: GlueContext, jdbc_connection_name: str = ""):
        self.glue_context = glue_context
        super().__init__(jdbc_connection_name)

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
        connection_options = {
            "url": self.jdbc_url_with_db(database),
            "user": self.jdbc_user,
            "password": self.jdbc_password,
            "dbtable": table,
        }
        return self.glue_context.create_dynamic_frame_from_options("mysql", connection_options)

    def write_df_from_jdbc(self,
                           dataframe: DataFrame | DynamicFrame,
                           database: str,
                           table: str,
                           *,
                           overwrite: bool = False,
                           ) -> None:
        if overwrite:
            if isinstance(dataframe, DynamicFrame):
                dataframe = dataframe.toDF()
            elif isinstance(dataframe, DataFrame):
                pass
            else:
                raise TypeError(f"Invalid type given dataframe: {dataframe}")
            dataframe.write\
                .format("jdbc")\
                .option("url", self.jdbc_url_with_db(database))\
                .option("dbtable", table)\
                .option("user", self.jdbc_user)\
                .option("password", self.jdbc_password)\
                .mode("overwrite")\
                .save()
        else:
            if isinstance(dataframe, DataFrame):
                write_dyf = DynamicFrame.fromDF(dataframe, self.glue_context, f"{database}.{table}")
            elif isinstance(dataframe, DynamicFrame):
                write_dyf = dataframe
            else:
                raise TypeError(f"Invalid type given dataframe: {dataframe}")
            self.glue_context.write_dynamic_frame.from_jdbc_conf(
                frame=write_dyf,
                catalog_connection=self.connection["Connection"]["Name"],
                connection_options={
                    "dbtable": table,
                    "database": database,
                }
            )
