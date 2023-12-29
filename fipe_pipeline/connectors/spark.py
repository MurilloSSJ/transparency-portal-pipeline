from pyspark.sql import SparkSession
import logging


class SparkConnector:
    def __init__(self, configuration: dict):
        self.logger = logging.getLogger(__name__)
        self.spark_config = SparkSession.builder.appName(f"fipe-extractor")
        self.add_session_config(configuration)

    def default(self):
        """Default configurations for basic execution"""
        self.spark_config.config("spark.sql.broadcastTimeout", "360000")
        self.spark_config.config(
            "spark.serializer", "org.apache.spark.serializer.KryoSerializer"
        )
        self.spark_config.config("spark.driver.memory", "12G")
        self.spark_config.config("spark.driver.maxResultSize", "4G")
        self.spark_config.config("spark.sql.debug.maxToStringFields", "100")
        self.spark_config.config("spark.ui.showConsoleProgress", "true")
        return self

    def awsfs(self, access_key: str, secret_key: str):
        """Sets up AWS S3 filesystem with hadoop. Requires the environment
        variables defining the `AWS_ACCESS_KEY_ID` and the
        `AWS_SECRET_ACCESS_KEY` to properly work

        Raises:
            AWSKeysNotFound: Could not find the required AWS credentials in the
                current environment

        """
        self.session_config.config(
            "spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.2"
        )
        self.session_config.config("spark.hadoop.fs.s3a.access.key", access_key)
        self.session_config.config("spark.hadoop.fs.s3a.secret.key", secret_key)
        self.session_config.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        return self

    def get(self) -> SparkSession:
        """Method to get the configured spark session

        Args:
            verbose: flag to specify if spark logger should be silenced

        Returns:
            the configured SparkSession

        """
        self.spark = self._builder()
        return self.spark

    def add_session_config(self, configuration: dict):
        if configuration:
            for key, value in configuration.items():
                self.spark_config.config(key, value)

    def __exit__(self):
        self._spark.stop()
        self._spark = None
