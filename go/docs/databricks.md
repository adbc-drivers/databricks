---
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
{}
---

{{ cross_reference|safe }}
# Databricks Driver {{ version }}

{{ heading|safe }}

This driver provides access to [Databricks][databricks]{target="_blank"}, a
cloud-based platform for data analytics.

## Installation

The Databricks driver can be installed with [dbc](https://docs.columnar.tech/dbc):

```bash
dbc install databricks
```

## Connecting

TODO: This section once https://github.com/apache/arrow-adbc/pull/3771 is merged here.

To connect, edit the `uri` option below to match your environment and run the following:

```python
from adbc_driver_manager import dbapi

conn = dbapi.connect(
  driver="databricks",
  db_kwargs = {
    "uri": "TODO"
  }
)
```

Note: The example above is for Python using the [adbc-driver-manager](https://pypi.org/project/adbc-driver-manager) package but the process will be similar for other driver managers.

### Connection String Format

TODO: This section once https://github.com/apache/arrow-adbc/pull/3771 is merged here.

## Feature & Type Support

{{ features|safe }}

### Types

{{ types|safe }}

## Compatibility

{{ compatibility_info|safe }}

## Previous Versions

To see documentation for previous versions of this driver, see the following:

- [v0.1.0](./v0.1.0.md)

{{ footnotes|safe }}

[databricks]: https://www.databricks.com
