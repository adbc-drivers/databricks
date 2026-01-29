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

import adbc_driver_manager.dbapi
from adbc_drivers_validation import model
from adbc_drivers_validation.tests.connection import (
    TestConnection as BaseTestConnection,
)
from adbc_drivers_validation.tests.connection import (
    generate_tests,
)

from . import databricks


class TestConnection(BaseTestConnection):
    def test_get_objects_schema(
        self, conn: adbc_driver_manager.dbapi.Connection, driver: model.DriverQuirks
    ) -> None:
        objects = (
            conn.adbc_get_objects(
                depth="db_schemas", catalog_filter=driver.features.current_catalog
            )
            .read_all()
            .to_pylist()
        )
        schemas = [
            (obj["catalog_name"], schema["db_schema_name"])
            for obj in objects
            for schema in obj["catalog_db_schemas"]
        ]
        assert list(sorted(set(schemas))) == list(sorted(schemas))
        assert (
            driver.features.current_catalog,
            driver.features.current_schema,
        ) in schemas

        objects = (
            conn.adbc_get_objects(
                depth="db_schemas",
                catalog_filter=driver.features.current_catalog,
                db_schema_filter=driver.features.current_schema,
            )
            .read_all()
            .to_pylist()
        )
        schemas = [
            (obj["catalog_name"], schema["db_schema_name"])
            for obj in objects
            for schema in obj["catalog_db_schemas"]
        ]
        assert list(sorted(set(schemas))) == list(sorted(schemas))
        assert (
            driver.features.current_catalog,
            driver.features.current_schema,
        ) in schemas

        objects = (
            conn.adbc_get_objects(
                depth="db_schemas", catalog_filter="thiscatalogdoesnotexist"
            )
            .read_all()
            .to_pylist()
        )
        schemas = [
            (obj["catalog_name"], schema["db_schema_name"])
            for obj in objects
            for schema in obj["catalog_db_schemas"]
        ]
        assert schemas == []

        objects = (
            conn.adbc_get_objects(
                depth="db_schemas",
                catalog_filter=driver.features.current_catalog,
                db_schema_filter="thiscatalogdoesnotexist",
            )
            .read_all()
            .to_pylist()
        )
        schemas = [
            (obj["catalog_name"], schema["db_schema_name"])
            for obj in objects
            for schema in obj["catalog_db_schemas"]
        ]
        assert schemas == []

    def test_get_objects_table_not_exist(
        self,
        conn: adbc_driver_manager.dbapi.Connection,
        driver: model.DriverQuirks,
    ) -> None:
        table_name = "getobjectstest2"
        with conn.cursor() as cursor:
            try:
                cursor.execute(driver.drop_table(table_name=table_name))
            except adbc_driver_manager.Error as e:
                if not driver.is_table_not_found(table_name=table_name, error=e):
                    raise

        objects = (
            conn.adbc_get_objects(
                depth="tables",
                catalog_filter=driver.features.current_catalog,
                db_schema_filter=driver.features.current_schema,
            )
            .read_all()
            .to_pylist()
        )
        tables = [
            (obj["catalog_name"], schema["db_schema_name"], table["table_name"])
            for obj in objects
            for schema in obj["catalog_db_schemas"]
            for table in schema["db_schema_tables"]
        ]
        assert list(sorted(set(tables))) == list(sorted(tables))
        table_id = (
            driver.features.current_catalog,
            driver.features.current_schema,
            table_name,
        )
        assert table_id not in tables

    def test_get_objects_table_present(
        self,
        conn: adbc_driver_manager.dbapi.Connection,
        driver: model.DriverQuirks,
        get_objects_table,
    ) -> None:
        table_id = get_objects_table
        objects = (
            conn.adbc_get_objects(
                depth="tables",
                catalog_filter=driver.features.current_catalog,
                db_schema_filter=driver.features.current_schema,
            )
            .read_all()
            .to_pylist()
        )
        tables = [
            (obj["catalog_name"], schema["db_schema_name"], table["table_name"])
            for obj in objects
            for schema in obj["catalog_db_schemas"]
            for table in schema["db_schema_tables"]
        ]
        assert list(sorted(set(tables))) == list(sorted(tables))
        assert table_id in tables

    def test_get_objects_table_invalid_schema(
        self,
        conn: adbc_driver_manager.dbapi.Connection,
        driver: model.DriverQuirks,
        get_objects_table,
    ) -> None:
        table_id = get_objects_table
        objects = (
            conn.adbc_get_objects(
                depth="tables",
                catalog_filter=driver.features.current_catalog,
                db_schema_filter="thiscatalogdoesnotexist",
            )
            .read_all()
            .to_pylist()
        )
        tables = [
            (obj["catalog_name"], schema["db_schema_name"], table["table_name"])
            for obj in objects
            for schema in obj["catalog_db_schemas"]
            for table in schema["db_schema_tables"]
        ]
        assert list(sorted(set(tables))) == list(sorted(tables))
        assert table_id not in tables

    def test_get_objects_table_invalid_table(
        self,
        conn: adbc_driver_manager.dbapi.Connection,
        driver: model.DriverQuirks,
        get_objects_table,
    ) -> None:
        table_id = get_objects_table
        objects = (
            conn.adbc_get_objects(
                depth="tables",
                catalog_filter=driver.features.current_catalog,
                db_schema_filter=driver.features.current_schema,
                table_name_filter="thiscatalogdoesnotexist",
            )
            .read_all()
            .to_pylist()
        )
        tables = [
            (obj["catalog_name"], schema["db_schema_name"], table["table_name"])
            for obj in objects
            for schema in obj["catalog_db_schemas"]
            for table in schema["db_schema_tables"]
        ]
        assert list(sorted(set(tables))) == list(sorted(tables))
        assert table_id not in tables

    def test_get_objects_table_exact_table(
        self,
        conn: adbc_driver_manager.dbapi.Connection,
        driver: model.DriverQuirks,
        get_objects_table,
    ) -> None:
        table_id = get_objects_table
        objects = (
            conn.adbc_get_objects(
                depth="tables",
                catalog_filter=driver.features.current_catalog,
                db_schema_filter=driver.features.current_schema,
                table_name_filter=table_id[2],
            )
            .read_all()
            .to_pylist()
        )
        tables = [
            (obj["catalog_name"], schema["db_schema_name"], table["table_name"])
            for obj in objects
            for schema in obj["catalog_db_schemas"]
            for table in schema["db_schema_tables"]
        ]
        assert list(sorted(set(tables))) == list(sorted(tables))
        assert table_id in tables

    def test_get_objects_column_not_exist(
        self,
        conn: adbc_driver_manager.dbapi.Connection,
        driver: model.DriverQuirks,
        get_objects_table,
    ) -> None:
        objects = (
            conn.adbc_get_objects(
                depth="columns",
                catalog_filter=driver.features.current_catalog,
                db_schema_filter=driver.features.current_schema,
            )
            .read_all()
            .to_pylist()
        )
        columns = [
            (
                obj["catalog_name"],
                schema["db_schema_name"],
                table["table_name"],
                column["column_name"],
            )
            for obj in objects
            for schema in obj["catalog_db_schemas"]
            for table in schema["db_schema_tables"]
            for column in table["table_columns"]
        ]
        assert list(sorted(set(columns))) == list(sorted(columns))
        table_id = (
            driver.features.current_catalog,
            driver.features.current_schema,
            "getobjectstest2",
        )
        for catalog, schema, table, column in columns:
            assert (catalog, schema, table) != table_id

    def test_get_objects_column_present(
        self,
        conn: adbc_driver_manager.dbapi.Connection,
        driver: model.DriverQuirks,
        get_objects_table,
    ) -> None:
        table_id = get_objects_table
        objects = (
            conn.adbc_get_objects(
                depth="columns",
                catalog_filter=driver.features.current_catalog,
                db_schema_filter=driver.features.current_schema,
            )
            .read_all()
            .to_pylist()
        )
        columns = [
            (
                obj["catalog_name"],
                schema["db_schema_name"],
                table["table_name"],
                column["column_name"],
            )
            for obj in objects
            for schema in obj["catalog_db_schemas"]
            for table in schema["db_schema_tables"]
            for column in table["table_columns"]
        ]
        assert list(sorted(set(columns))) == list(sorted(columns))
        assert (*table_id, "ints") in columns
        assert (*table_id, "strs") in columns

    def test_get_objects_column_filter_column_name(
        self,
        conn: adbc_driver_manager.dbapi.Connection,
        driver: model.DriverQuirks,
        get_objects_table,
    ) -> None:
        table_id = get_objects_table
        objects = (
            conn.adbc_get_objects(
                depth="columns",
                catalog_filter=driver.features.current_catalog,
                db_schema_filter=driver.features.current_schema,
                column_name_filter="ints",
            )
            .read_all()
            .to_pylist()
        )
        columns = [
            (
                obj["catalog_name"],
                schema["db_schema_name"],
                table["table_name"],
                column["column_name"],
            )
            for obj in objects
            for schema in obj["catalog_db_schemas"]
            for table in schema["db_schema_tables"]
            for column in table["table_columns"]
        ]
        assert list(sorted(set(columns))) == list(sorted(columns))
        assert (*table_id, "ints") in columns
        assert (*table_id, "strs") not in columns

    def test_get_objects_column_filter_table_name(
        self,
        conn: adbc_driver_manager.dbapi.Connection,
        driver: model.DriverQuirks,
        get_objects_table,
    ) -> None:
        table_id = get_objects_table
        objects = (
            conn.adbc_get_objects(
                depth="columns",
                catalog_filter=driver.features.current_catalog,
                db_schema_filter=driver.features.current_schema,
                table_name_filter=table_id[-1],
            )
            .read_all()
            .to_pylist()
        )
        columns = [
            (
                obj["catalog_name"],
                schema["db_schema_name"],
                table["table_name"],
                column["column_name"],
            )
            for obj in objects
            for schema in obj["catalog_db_schemas"]
            for table in schema["db_schema_tables"]
            for column in table["table_columns"]
        ]
        assert list(sorted(set(columns))) == list(sorted(columns))
        assert (*table_id, "ints") in columns
        assert (*table_id, "strs") in columns
        assert len(columns) == 2


def pytest_generate_tests(metafunc) -> None:
    return generate_tests(databricks.QUIRKS, metafunc)
