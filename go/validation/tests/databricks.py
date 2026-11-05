# Copyright (c) 2025 ADBC Drivers Contributors
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

import functools
import re
from pathlib import Path

import sqlglot
from adbc_drivers_validation import model
from sqlglot.tokens import TokenType


class DatabricksQuirks(model.DriverQuirks):
    name = "databricks"
    driver = "adbc_driver_databricks"
    driver_name = "ADBC Driver Foundry Driver for Databricks"
    vendor_name = "Databricks"
    vendor_version = re.compile(r"(17\.[0-9]+.*|2026\.[0-9]+)")
    short_version = "17"
    features = model.DriverFeatures(
        connection_get_table_schema=False,
        connection_set_current_catalog=False,
        connection_set_current_schema=False,
        connection_transactions=False,
        get_objects=True,
        get_objects_constraints_check=False,
        get_objects_constraints_foreign=False,
        get_objects_constraints_primary=False,
        get_objects_constraints_unique=False,
        statement_bind=True,
        statement_bulk_ingest=True,
        statement_bulk_ingest_catalog=False,
        statement_bulk_ingest_schema=False,
        statement_bulk_ingest_temporary=False,
        statement_execute_schema=False,
        statement_get_parameter_schema=False,
        statement_prepare=True,
        statement_rows_affected=True,
        statement_rows_affected_ddl=True,
        current_catalog="main",
        current_schema="adbc_testing",
        supported_xdbc_fields=[],
    )
    setup = model.DriverSetup(
        database={
            "databricks.server_hostname": model.FromEnv("DATABRICKS_HOST"),
            # "databricks.access_token": model.FromEnv("DATABRICKS_ACCESSTOKEN"),
            "databricks.oauth.client_id": model.FromEnv("DATABRICKS_OAUTH_CLIENT_ID"),
            "databricks.oauth.client_secret": model.FromEnv(
                "DATABRICKS_OAUTH_CLIENT_SECRET"
            ),
            "databricks.http_path": model.FromEnv("DATABRICKS_HTTPPATH"),
            "databricks.catalog": "main",
            "databricks.schema": "adbc_testing",
        },
        connection={},
        statement={},
    )

    @property
    def queries_paths(self) -> tuple[Path]:
        return (Path(__file__).parent.parent / "queries",)

    def bind_parameter(self, index: int) -> str:
        return "?"

    def query_override(self, context: str, default: str) -> str:
        if context == "TestStatement.sample_table":
            return default.replace("VARCHAR", "STRING")
        return super().query_override(context, default)

    def is_table_not_found(self, table_name: str, error: Exception) -> bool:
        # Check if the error indicates a table not found condition
        error_str = str(error).lower()
        return (
            "table" in error_str
            and (
                "does not exist" in error_str
                or "doesn't exist" in error_str
                or "not found" in error_str
                or "cannot be found" in error_str
            )
            and table_name.lower() in error_str
        )

    def quote_one_identifier(self, identifier: str) -> str:
        identifier = identifier.replace("`", "``")
        return f"`{identifier}`"

    def split_statement(self, statement: str) -> list[str]:
        # SQLGlot's Databricks renderer drops TIMESTAMP(p) precision.
        # Split at tokens instead, preserving the original SQL and ignoring
        # semicolons inside literals and comments.
        statements = []
        start = None
        for token in sqlglot.tokenize(statement, read=self.name):
            if token.token_type == TokenType.SEMICOLON:
                if start is not None:
                    statements.append(statement[start : token.start].strip())
                    start = None
            elif start is None:
                start = token.start
        if start is not None:
            statements.append(statement[start:].strip())
        return statements


@functools.cache
def get_quirks(version: str) -> DatabricksQuirks:
    quirks = DatabricksQuirks()
    if version not in {"latest", quirks.short_version}:
        raise ValueError(f"Unsupported Databricks version: {version}")
    return quirks
