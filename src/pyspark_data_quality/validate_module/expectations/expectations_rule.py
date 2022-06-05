from __future__ import annotations
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from typing import Dict, Any, List


"""
great_expectations rules reference: 
https://docs.greatexpectations.io/docs/guides/expectations/how_to_create_and_edit_expectations_with_a_profiler#semantic-types-dictionary-configuration
"""

@dataclass
class CommonFields:
    """Common fields that use in expectation suite. 
    Do not change this unless necessary
    """
    data_asset_type: str = None
    expectation_suite_name: str = None
    ge_cloud_id: str = None
    meta: Dict[str, str] = field(default_factory=lambda: {"great_expectations_version": "0.15.3"})


@dataclass
class BaseExpectation(ABC):
    """A common interface for construct expectation

    Args:
        ABC (_type_): _description_

    Raises:
        NotImplementedError: _description_
    """
    expectation_type: str = field(default=None)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    meta: Dict[str, str] = field(default_factory=dict)
    
    @abstractmethod
    def create(self) -> None:
        raise NotImplementedError


@dataclass
class OrderedColumnsMatchExpectation(BaseExpectation):
    """Expect the columns to exactly match a specified list.

    Args:
        column_list (List[str]): The column names, in the correct order.
    """

    def create(self, column_list: List[str]) -> OrderedColumnsMatchExpectation:
        rule_name = "expect_table_columns_to_match_ordered_list"

        return OrderedColumnsMatchExpectation(
            expectation_type=rule_name,
            kwargs={"column_list": column_list}
        )


@dataclass
class ColumnsMatchExpectation(BaseExpectation):
    """Expect the columns to exactly match an unordered set.

    Args:
        column_set (list of str): The column names, in the correct order.
        exact_match (boolean): Default is True. Whether the list of columns must exactly match the observed columns.
    """

    def create(self, column_set: List[str], exact_match: bool = True) -> OrderedColumnsMatchExpectation:
        rule_name = "expect_table_columns_to_match_set"

        return OrderedColumnsMatchExpectation(
            expectation_type=rule_name,
            kwargs={"column_set": column_set, "exact_match": exact_match}
        )


@dataclass
class RowCountBetweenExpectation(BaseExpectation):
    """Expect the number of rows to be between two values.

    Args:
        min_rows (int): The minimum number of rows, inclusive.
        max_rows (int): The maximum number of rows, inclusive.
    """

    def create(self, min_rows: int, max_rows: int) -> RowCountBetweenExpectation:
        rule_name = "expect_table_row_count_to_be_between"

        return RowCountBetweenExpectation(
            expectation_type=rule_name,
            kwargs={"min_value": min_rows, "max_value": max_rows}
        )


@dataclass
class ValuesNotNullExpectation(BaseExpectation):
    """Expect column values to not be null.

    Args:
        column_name (str): The column name
    """

    def create(self, column_name) -> ValuesNotNullExpectation:
        rule_name = "expect_column_values_to_not_be_null"

        return ValuesNotNullExpectation(
            expectation_type=rule_name,
            kwargs={"column": column_name}
        )

@dataclass
class ColumnCountEqualExpectation(BaseExpectation):
    """Expect the number of columns to equal a value.

    Args:
        column_count (str): The expected number of columns.
    """
    def create(self, column_count: int) -> ColumnCountEqualExpectation:
        rule_name = "expect_table_column_count_to_equal"
        
        return ColumnCountEqualExpectation(
            expectation_type=rule_name,
            kwargs={"value": column_count}
        )

@dataclass
class ColumnTypeMatchExpectation(BaseExpectation):
    """Expect a column to contain values of a specified data type.

    Args:
        column_name (str): The column name
        column_type (str): A string representing the data type that each column should have as entries. For example, str, int or bool
    """
    def create(self, column_name: str, column_type: str) -> ColumnTypeMatchExpectation:
        rule_name = "expect_column_values_to_be_of_type"
        
        return ColumnTypeMatchExpectation(
            expectation_type=rule_name,
            kwargs={"column": column_name, "type_": column_type}
        )

@dataclass
class ColumnTypeListMatchExpectation(BaseExpectation):
    """Expect a column to contain values from a specified type list.

    Args:
        column_name (str): The column name
        column_type (str): A list of strings representing the data type that each column should have as entries. For example, ["TEXT", "STRING", "VARCHAR"]
    """
    def create(self, column_name: str, column_type_list: str) -> ColumnTypeMatchExpectation:
        rule_name = "expect_column_values_to_be_in_type_list"
        
        return ColumnTypeMatchExpectation(
            expectation_type=rule_name,
            kwargs={"column": column_name, "type_list": column_type_list}
        )

@dataclass
class DateTimeFormatMatchExpectation(BaseExpectation):
    """Expect column entries to be strings representing a date or time with a given format.

    Args:
        column_name (str): The column name
        dt_format (str): A strftime format string to use for matching. For example, %Y-%m-%d
    """
    def create(self, column_name: str, dt_format: str) -> DateTimeFormatMatchExpectation:
        rule_name = "expect_column_values_to_match_strftime_format"
        
        return DateTimeFormatMatchExpectation(
            expectation_type=rule_name,
            kwargs={"column": column_name, "strftime_format": dt_format}
        )   
