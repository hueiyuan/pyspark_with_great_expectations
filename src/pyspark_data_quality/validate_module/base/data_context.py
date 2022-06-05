from abc import ABC, abstractmethod
from typing import Dict, Any
from dataclasses import dataclass, field

from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DatasourceConfig

@dataclass
class BaseContext(ABC):
    """
      Define interface class of data context
    """
    data_source_name: str = field(init=False)
    expectations_store_name: str = field(init=False)
    validations_store_name: str = field(init=False)
    checkpoints_store_name: str = field(init=False)
    evaluation_store_name: str = field(init=False)
      
    @abstractmethod
    def data_source(self) -> Dict[str, DatasourceConfig]:
        """Configure data source"""
        raise NotImplementedError

    @abstractmethod
    def stores(self) -> Dict[str, Any]:
        """Define store for expectation, data docs, checkpoint and evaluation parameter"""
        raise NotImplementedError

    @abstractmethod
    def data_docs(self) -> Dict[str, Dict[str, Any]]:
        """Define data docs site"""
        raise NotImplementedError

    @abstractmethod
    def validation_operators(self) -> Dict[str, Dict[str, Any]]:
        """Define post validation actions"""
        raise NotImplementedError

    @abstractmethod
    def build(self) -> BaseDataContext:
        """Build data context based on configurations"""
        raise NotImplementedError
