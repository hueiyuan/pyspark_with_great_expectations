import argparse
from dataclasses import dataclass

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from pyspark_data_quality.libs.utils import Environment
from pyspark_data_quality.validate_module.base.data_asset import DataAssetName
from pyspark_data_quality.validate_module.custom.validator import Validator

@dataclass
class TransformExample:
    env: str
    spark_session: SparkSession
    s3_source_bucket: str
    s3_source_prefix: str
    s3_destination_bucket: str
    s3_destination_prefix: str
    logger: str
    
    def __post_init__(self):
        self._input_path = f"s3://{self.s3_source_bucket}/{self.s3_source_prefix}"
        self._output_path = f"s3://{self.s3_destination_bucket}/{self.s3_destination_prefix}"
    
    def load_source_data(self) -> DataFrame:
        _source_df = self.spark_session.read.format("parquet") \
                                            .load(self._input_path)
        
        self.logger.info('Load dataf from s3.')
        return _source_df
    
    def transform_logics(self, _df: DataFrame) -> DataFrame:
        """
            implement dataframe logics
        """
        _df = _df.filter(F.col('col1') == 'xxx')\
                .withColumnRenamed('old_col', 'col4')
        
        _processed_df = _df.select(['col1', 'col2', 'col3', 'col4', "dt"])\
                        .repartition(10)
        
        self.logger.info('Completely transform dataframe.')
        return _processed_df
    
    def save_processed_data(self, 
                               _df: DataFrame) -> None:
        _df.write.format("parquet") \
                .mode("overwrite")\
                .partitionBy("dt")\
                .save(self._output_path)
                
        self.logger.info('Completely save to s3.')
    
    def run(self):
        _source_df = self.load_source_data()
        _processed_df = self.transform_logics(_source_df)
        
        ## Execute greate_expectation data quality and validation
        data_asset_name = DataAssetName(
            table_name='custom_table',
            dt='2022-06-05')

        validator = Validator(
            env=self.env,
            asset_name=str(data_asset_name), 
            df=_processed_df, 
            suite_name='custom_table_validation_suite')
        validator.run()
        
        if validator.status:
            self.save_processed_data(_processed_df)
            self.logger.info("Creation table is completed.")
        else:
            self.logger.info("Validation Failed and alert to Slack.")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--environment",
        action="store",
        type=Environment,
        required=True,
        choices=list(Environment),
        help="Which environment?",
    )
    args = parser.parse_args()
    
    spark = (
        SparkSession.builder.appName("Pysprk data quality Example")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )

    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)

    example_transform = TransformExample(
        env=args.environment.value,
        spark_session=spark,
        s3_source_bucket='source_bucket',
        s3_source_prefix='your/source/data/prefix',
        s3_destination_bucket='destination_bucket',
        s3_destination_prefix='your/destination/data/prefix',
        logger=logger
    )
    
    example_transform.run()

if __name__ == "__main__":
    main()
    
    
