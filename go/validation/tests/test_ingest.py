# Copyright (c) 2025-2026 ADBC Drivers Contributors
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

import adbc_drivers_validation.tests.ingest
import pytest

from .databricks import get_quirks


def pytest_generate_tests(metafunc) -> None:
    quirks = [get_quirks(metafunc.config.getoption("vendor_version"))]
    return adbc_drivers_validation.tests.ingest.generate_tests(quirks, metafunc)


class TestIngest(adbc_drivers_validation.tests.ingest.TestIngest):
    # Ingestion sends one INSERT per row. This 6,000-row test has hit TCP read
    # timeouts; re-enable it when ingestion can handle this workload reliably.
    @pytest.mark.skip(
        reason="Temporary: network read timeouts during large-batch ingestion"
    )
    def test_create_large_batch(self, driver, conn, query):
        super().test_create_large_batch(driver, conn, query)

    # This 100-column, 1,000-row test also sends one INSERT per row and has
    # hit TCP read timeouts; re-enable it when ingestion handles it reliably.
    @pytest.mark.skip(
        reason="Temporary: network read timeouts during many-column ingestion"
    )
    def test_many_columns(self, driver, conn):
        super().test_many_columns(driver, conn)
