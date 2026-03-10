<!--
  Copyright (c) 2025 ADBC Drivers Contributors

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

          http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# ADBC Drivers for Databricks

This repository contains [ADBC drivers](https://arrow.apache.org/adbc/) for
Databricks, implemented in different languages.

## For Power BI

If you use [Power BI](https://www.microsoft.com/power-platform/products/power-bi),
follow these instructions to connect to Databricks using the C#-based ADBC driver:

- [Arrow Database Connectivity (ADBC) driver for Power BI](https://docs.databricks.com/partners/bi/power-bi-adbc)

## For Other Applications

Prebuilt versions of the Go-based ADBC driver are available from the
[Columnar](https://columnar.tech) driver registry. These can be
installed using [dbc](https://columnar.tech/dbc):

```sh
dbc install databricks
```

After installing the Go-based driver using dbc, see examples showing how
to use it with various languages:

- [C++](https://github.com/columnar-tech/adbc-quickstarts/tree/main/cpp/databricks)
- [Go](https://github.com/columnar-tech/adbc-quickstarts/tree/main/go/databricks)
- [Java](https://github.com/columnar-tech/adbc-quickstarts/tree/main/java/databricks)
- [Python](https://github.com/columnar-tech/adbc-quickstarts/tree/main/python/databricks)
- [R](https://github.com/columnar-tech/adbc-quickstarts/tree/main/r/databricks)
- [Rust](https://github.com/columnar-tech/adbc-quickstarts/tree/main/rust/databricks)

See [Building](#building) if you would rather build the drivers yourself.

## Building

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
# Test scenario 1
