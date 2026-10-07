# Copyright (c) 2026 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pyarrow
import pytest
from adbc_drivers_validation import model

from .databricks import DatabricksQuirks


def query_set():
    return model.query_set(DatabricksQuirks().queries_paths)


@pytest.mark.parametrize(
    "kind", ["binary", "binary_view", "large_binary", "fixed_size_binary"]
)
def test_binary_binding_ddl(kind):
    case = query_set().queries[f"type/bind/{kind}"]
    assert "res BINARY" in case.query.setup_query()
    assert "BINARY(" not in case.query.setup_query()
    assert not case.pytest_marks


@pytest.mark.parametrize("kind", ["timestamp", "timestamptz"])
@pytest.mark.parametrize("unit", ["s", "ms", "us", "ns"])
def test_timestamp_binding_precision(kind, unit):
    case = query_set().queries[f"type/bind/{kind}_{unit}"]
    bound = case.query.bind_data().sort_by([("idx", "ascending")])
    expected = case.query.expected_result()
    assert not case.pytest_marks
    for field in expected.schema:
        assert field.type.unit == "us"
        assert field.type.tz == ("Etc/UTC" if kind == "timestamptz" else None)
        values = bound.column(field.name).cast(pyarrow.int64()).to_pylist()
        if unit == "ns":
            values = [value // 1000 if value is not None else None for value in values]
        else:
            multiplier = {"s": 1000000, "ms": 1000, "us": 1}[unit]
            values = [
                value * multiplier if value is not None else None for value in values
            ]
        assert expected.column(field.name).cast(pyarrow.int64()).to_pylist() == values


def test_decimal_binding_returns_exact_strings():
    case = query_set().queries["type/bind/decimal"]
    assert not case.pytest_marks
    assert case.query.expected_schema().field("res").type == pyarrow.string()
    assert case.query.expected_result().column("res").to_pylist() == [
        None,
        "-999.99",
        "0.00",
        "123.45",
        "9999999.99",
    ]


@pytest.mark.parametrize("unit", ["s", "ms", "us", "ns"])
def test_time_binding_is_explicitly_unsupported(unit):
    case = query_set().queries[f"type/bind/time_{unit}"]
    assert "does not support TIME" in case.metadata()["skip"]
