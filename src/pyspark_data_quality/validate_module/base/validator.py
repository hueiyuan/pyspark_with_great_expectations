from dataclasses import dataclass, field
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult

from .data_asset import DataAssetName

@dataclass
class BaseValidator(ABC):
    """Interface class of validator
    """
    df: DataFrame
    asset_name: DataAssetName
    suite_name: str
    env: str

    @abstractmethod
    def run(self) -> None:
        """Perform validation"""
        raise NotImplementedError
    
    @abstractmethod
    def result(self) -> CheckpointResult:
        """Checkpoint result"""
        raise NotImplementedError
    
    @abstractmethod
    def status(self) -> bool:
        """Status of validation result"""
        raise NotImplementedError
