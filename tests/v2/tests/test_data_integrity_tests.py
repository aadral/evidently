import pandas as pd

import pytest

from evidently.pipeline.column_mapping import ColumnMapping
from evidently.v2.tests import TestNumberOfColumns
from evidently.v2.tests import TestNumberOfRows
from evidently.v2.tests import TestNumberOfNulls
from evidently.v2.test_suite import TestSuite


def test_data_integrity_test_number_of_columns() -> None:
    test_dataset = pd.DataFrame(
        {
            "category_feature": ["n", "d", "p", "n"],
            "numerical_feature": [0, 2, 2, 432],
            "target": [0, 0, 0, 1]
        }
    )
    suite = TestSuite(tests=[TestNumberOfColumns(gte=10)])
    suite.run(current_data=test_dataset, reference_data=None, column_mapping=ColumnMapping())
    assert not suite

    suite = TestSuite(tests=[TestNumberOfColumns(eq=3)])
    suite.run(current_data=test_dataset, reference_data=None, column_mapping=ColumnMapping())
    assert suite


def test_data_integrity_test_number_of_rows() -> None:
    test_dataset = pd.DataFrame(
        {
            "target": [0, 0, 0, 1]
        }
    )
    suite = TestSuite(tests=[TestNumberOfRows(is_in=[10, 3, 50])])
    suite.run(current_data=test_dataset, reference_data=None, column_mapping=ColumnMapping())
    assert not suite

    suite = TestSuite(tests=[TestNumberOfRows(gte=4)])
    suite.run(current_data=test_dataset, reference_data=None, column_mapping=ColumnMapping())
    assert suite


@pytest.mark.parametrize(
    "test_dataset, conditions, result",
    (
        (
            pd.DataFrame(
                {
                    "target": [0, 0, 0, 1]
                }
            ),
            {"eq": 0},
            True
        ),
        (
            pd.DataFrame(
                {
                    "target": [0, 0, None, 1],
                    "numeric": [None, None, None, 1]
                }
            ),
            {"lt": 3},
            False
        ),
    )
)
def test_data_integrity_test_number_of_nulls_no_errors(
        test_dataset: pd.DataFrame, conditions: dict, result: bool
) -> None:
    suite = TestSuite(tests=[TestNumberOfNulls(**conditions)])
    suite.run(current_data=test_dataset, reference_data=None, column_mapping=ColumnMapping())
    assert bool(suite) is result