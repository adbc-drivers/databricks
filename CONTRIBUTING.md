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

# How to Contribute

All contributors are expected to follow the [Code of
Conduct](https://github.com/adbc-drivers/databricks?tab=coc-ov-file#readme).

## Reporting Issues and Making Feature Requests

Please file issues and feature requests on the GitHub issue tracker:
https://github.com/adbc-drivers/databricks/issues

Potential security vulnerabilities should be reported to
[security@adbc-drivers.org](mailto:security@adbc-drivers.org) instead.  See
the
[Security Policy](https://github.com/adbc-drivers/databricks?tab=security-ov-file#readme).

## Build and Test

### C#

For basic development, the driver can be built and tested like any .NET
project.  From the `csharp/` subdirectory:

```shell
$ dotnet build
$ dotnet test
```

## Opening a Pull Request

Before opening a pull request:

- Review your changes and make sure no stray files, etc. are included.
- Ensure the Apache license header is at the top of all files.
- Check if there is an existing issue.  If not, please file one, unless the
  change is trivial.
- Assign the issue to yourself by commenting just the word `take`.
- Run the static checks by installing [pre-commit](https://pre-commit.com/),
  then running `pre-commit run --all-files` from inside the repository.  Make
  sure all your changes are staged/committed (unstaged changes will be
  ignored).

When writing the pull request description:

- Ensure the title follows [Conventional
  Commits](https://www.conventionalcommits.org/en/v1.0.0/) format.  The
  component should be `csharp` if it affects the C# driver, or it can be
  omitted for general maintenance (in general: it should be a directory path
  relative to the repo root, e.g. `csharp/auth` would also be valid if that
  directory existed).  Example titles:

  - `feat(csharp): support GEOGRAPHY data type`
  - `chore: update action versions`
  - `fix!(csharp): return us instead of ms`

  Ensure that breaking changes are appropriately flagged with a `!` as seen
  in the last example above.
- Make sure the description ends with `Closes #NNN`, `Fixes #NNN`, or
  similar, so that the issue will be linked to your pull request.
