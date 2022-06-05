from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import List, Dict

from ..libs.utils import s3_client
from .expectations.expectatione_rules import (
    BaseExpectation,
    CommonFields
)

@dataclass
class ValidationSuiteGenerator():
    """Validation suite generator that uses combination of expectations"""
    env: str = None
    expectation_suite_name: str = None
    expectations: List[BaseExpectation] = field(default_factory=list)
    _result: Dict = None
    
    def add_expectation(self, expectation: BaseExpectation) -> ValidationSuiteGenerator:
        self.expectations.append(expectation)

        return self

    def build(self) -> ValidationSuiteGenerator:
        if self.expectations is None:
            raise Exception("No expectation being added.")

        if self.expectation_suite_name is None:
            raise Exception("Suite name must be set.")

        self._common_fields = CommonFields(
            expectation_suite_name=self.expectation_suite_name)

        common_fields_dict = asdict(self._common_fields)
        expectations_dict = {"expectations": list(
            map(lambda e: asdict(e), self.expectations))}

        merge_dicts = {**common_fields_dict, **expectations_dict}
        
        self._result = merge_dicts
        
        return self
    
    def save_to_store(self):
        object_s3_path = f"{self.env}/validations/expectations_store/{self.expectation_suite_name}.json"
        
        s3_client().save_to_s3(
            data=self._result,
            bucket_name='s3_expectation_bucket_name',
            object_key_name=object_s3_path
        )
        
