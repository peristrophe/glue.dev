from abc import ABC
from urllib.parse import urlparse
import boto3

class JDBCConnectionContainer(ABC):

    connection: dict

    def __init__(self, jdbc_connection_name: str):
        self.connection = {}
        if jdbc_connection_name:
            self.set_connection(jdbc_connection_name)

    def has_connection(self) -> bool:
        return len(self.connection) > 0

    def set_connection(self, jdbc_connection_name: str) -> None:
        # NOTE Require vpc endpoint for glue endpoint
        client = boto3.client("glue")
        self.connection = client.get_connection(Name=jdbc_connection_name)

    @property
    def connection_name(self) -> str:
        if not self.has_connection():
            raise AttributeError(f"{self.__class__.__name__} instance has not connection metadata.")
        return self.connection["Connection"]["Name"]

    @property
    def jdbc_user(self) -> str:
        if not self.has_connection():
            raise AttributeError(f"{self.__class__.__name__} instance has not connection metadata.")
        return self.connection["Connection"]["ConnectionProperties"]["USERNAME"]

    @property
    def jdbc_password(self) -> str:
        if not self.has_connection():
            raise AttributeError(f"{self.__class__.__name__} instance has not connection metadata.")
        return self.connection["Connection"]["ConnectionProperties"]["PASSWORD"]

    @property
    def jdbc_hostname(self) -> str:
        if not self.has_connection():
            raise AttributeError(f"{self.__class__.__name__} instance has not connection metadata.")
        url: str = self.connection["Connection"]["ConnectionProperties"]["JDBC_CONNECTION_URL"]
        if url.startswith("jdbc:"):
            url = url.replace("jdbc:", "")
        parsed_url = urlparse(url)
        return parsed_url.hostname

    def jdbc_url_with_db(self, database: str) -> str:
        if not self.has_connection():
            raise AttributeError(f"{self.__class__.__name__} instance has not connection metadata.")
        url = self.connection["Connection"]["ConnectionProperties"]["JDBC_CONNECTION_URL"].split("/")
        url[-1] = database
        return "/".join(url)
