import argparse

from pyspark_data_quality.libs.utils import Environment
from pyspark_data_quality.validate_module.expectation_suite_generator import ValidationSuiteGenerator
from pyspark_data_quality.validate_module.expectations.expectatione_rules import (
    OrderedColumnsMatchExpectation, 
    ColumnsMatchExpectation,
    RowCountBetweenExpectation, 
    ValuesNotNullExpectation
)

class SuiteGenerate:
    """
    validation suite which built from different of expectations combination
    """
    def __init__(self, env):
        self._env = env
        self.dataframe_cols = [
            'col1',
            'col2',
            'col3',
            'col4',
            'dt'
        ]
        self.dataframe_suite_name = "dataframe_validation_suite"
    
    def run(self):
        ordered_column_exp = OrderedColumnsMatchExpectation() \
            .create(column_list=self.dataframe_cols)
            
        column_exp = ColumnsMatchExpectation() \
            .create(
                column_set=self.dataframe_cols, 
                exact_match=False)

        row_count_exp = RowCountBetweenExpectation() \
            .create(min_rows=10000, max_rows=50000)

        vsg = (
            ValidationSuiteGenerator(
                env=self._env,
                expectation_suite_name=self.dataframe_suite_name)
                .add_expectation(ordered_column_exp)
                .add_expectation(column_exp)
                .add_expectation(row_count_exp)
                .build()
        )
        
        vsg.save_to_store()


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

    
    ## upload expectation json to s3
    SuiteGenerate(
        env=args.environment.value
    ).run()


if __name__ == "__main__":
    main()
