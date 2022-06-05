from dataclasses import dataclass

from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.core.batch import RuntimeBatchRequest

from .s3_data_context import S3Context
from ..base.validator import BaseValidator
from ...libs.utils import ssm_client


@dataclass
class Validator(BaseValidator):
    """A basic validator that perform validation using spark DataFrame
    """
    _result: CheckpointResult = None
    
    def __post_init__(self):
        self.context = S3Context(env=self.env).build()
        self.slack_alert_token = ssm_client().get_parameter_value(
            'ap-northeast-1',
            'your-ssm/slack-token')

    def run(self) -> None:
        batch_request = RuntimeBatchRequest(
            datasource_name="spark_runtime_data_source",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name=self.asset_name,
            batch_identifiers={"default_identifier_name": "some_identifier"},
            runtime_parameters={"batch_data": self.df}
        )

        checkpoint_config = {
            "name": f"{self.suite_name}_checkpoint",
            "config_version": 1,
            "class_name": "SimpleCheckpoint",
            "expectation_suite_name": self.suite_name
        }

        self.context.add_checkpoint(**checkpoint_config)

        slack_notification = {
            "name": "send_slack_notification_on_validation_result",
            "action": {
                    "class_name": "SlackNotificationAction",
                    "slack_webhook": f"https://hooks.slack.com/services/{self.slack_alert_token}",
                    "notify_on": "failure",
                    "notify_with": ["s3_site"],
                    "renderer": {
                        "module_name": "great_expectations.render.renderer.slack_renderer",
                        "class_name": "SlackRenderer"
                    }
            }
        }

        self._result = self.context.run_checkpoint(
            checkpoint_name=f"{self.suite_name}_checkpoint",
            validations=[{"batch_request": batch_request}],
            action_list=[slack_notification],
            run_name=f"{self.asset_name}"
        )

    @property
    def result(self) -> CheckpointResult:
        return self._result

    @property
    def status(self) -> bool:
        return self._result['success']
