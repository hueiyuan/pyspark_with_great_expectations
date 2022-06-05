from dataclasses import dataclass
from typing import Any, Dict

from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig

from ..base.data_context import BaseContext

@dataclass
class S3Context(BaseContext):
    """
      Define data context that use AWS S3 as storage backend
    """
    env: str = None
    s3_bucket: str = "s3_bucket_name"
    s3_validation_bucket: str = "s3_validation_bucket_name"
    s3_prefix: str = "{env}/validations/"
    expectations_store_name: str = "expectations_S3_store"
    checkpoints_store_name: str = "checkpoints_S3_store"
    validations_store_name: str = "validations_S3_store"
    evaluation_store_name: str = "evaluation_parameter_store"

    def __post_init__(self):
        if self.env is None:
            raise ValueError("Need to specific environment!")
        self.s3_prefix = self.s3_prefix.format(env=self.env)

    def data_source(self) -> Dict[str, DatasourceConfig]:
        datasources = {
            "spark_runtime_data_source": DatasourceConfig(
                class_name="Datasource",
                execution_engine={
                    "class_name": "SparkDFExecutionEngine",
                    "force_reuse_spark_context": True},
                data_connectors={
                    "default_runtime_data_connector_name": {
                        "class_name": "RuntimeDataConnector",
                        "batch_identifiers": ["default_identifier_name"]
                    }
                }
            )
        }

        return datasources

    def stores(self) -> Dict[str, Any]:
        stores = {
            self.expectations_store_name: {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": self.s3_bucket,
                    "prefix": self.s3_prefix + "expectations_store"
                }
            },
            self.validations_store_name: {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": self.s3_bucket,
                    "prefix": self.s3_prefix + "validations_store"
                },
            },
            self.checkpoints_store_name: {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": self.s3_bucket,
                    "prefix": self.s3_prefix + "checkpoints_store"
                },
            },
            self.evaluation_store_name: {"class_name": "EvaluationParameterStore"}}

        return stores

    def data_docs(self) -> Dict[str, Dict[str, Any]]:
        data_docs_sites = {
            "s3_site": {
                "class_name": "SiteBuilder",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": self.s3_validation_bucket,
                    "prefix": f"{self.env}/validations/data_docs_sites",
                },
                "site_index_builder": {
                    "class_name": "DefaultSiteIndexBuilder",
                    "show_cta_footer": True,
                }
            }
        }

        return data_docs_sites

    def validation_operators(self) -> Dict[str, Dict[str, Any]]:
        store_validation_result = {
            "name": "store_validation_result",
            "action": {"class_name": "StoreValidationResultAction"}
        }

        store_evaluation_params = {
            "name": "store_evaluation_params",
            "action": {"class_name": "StoreEvaluationParametersAction"}
        }

        update_data_docs = {
            "name": "update_data_docs",
            "action": {"class_name": "UpdateDataDocsAction"}
        }

        validation_operators = {
            "action_list_operator": {
                "class_name": "ActionListValidationOperator",
                "action_list": [
                    store_validation_result,
                    store_evaluation_params,
                    update_data_docs
                ]
            }
        }

        return validation_operators

    def build(self) -> BaseDataContext:
        data_context_config = DataContextConfig(
            config_version=2,
            plugins_directory=None,
            config_variables_file_path=None,
            datasources=self.data_source(),
            stores=self.stores(),
            expectations_store_name=self.expectations_store_name,
            validations_store_name=self.validations_store_name,
            checkpoint_store_name=self.checkpoints_store_name,
            evaluation_parameter_store_name=self.evaluation_store_name,
            data_docs_sites=self.data_docs(),
            validation_operators=self.validation_operators())

        return BaseDataContext(project_config=data_context_config)
