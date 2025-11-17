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

# CI/CD Workflows

This directory contains GitHub Actions workflows for automated testing and validation.

## Workflows

### C# Build and Test (`csharp.yml`)

**Purpose**: Builds and tests the C# Databricks ADBC driver across multiple platforms.

**Triggers**:
- Push to `main` or `maint-*` branches
- Pull requests (including from forks) to `main` or `maint-*` branches
- Only runs when C# code or the workflow itself changes

**Security**: Uses `pull_request_target` with safe checkout to support fork PRs in private repositories while maintaining security by checking out the exact PR commit SHA.

**Test Matrix**:
- **Operating Systems**: Ubuntu Latest, Windows Server 2022, macOS 13 (Intel), macOS Latest (ARM)
- **.NET Version**: 8.0.x
- **Timeout**: 15 minutes per job

**Steps**:
1. Checkout repository with submodules (uses exact PR commit SHA for safety)
2. Setup .NET SDK
3. Build using `ci/scripts/csharp_build.sh`
4. Test using `ci/scripts/csharp_test.sh`

**Notes**:
- Workflow skips if PR title contains "WIP"
- Tests both the main driver and Databricks-specific unit tests
- **Fork PRs**: Automatically runs on fork PRs by using `pull_request_target` with safe checkout
- **Security**: Checks out the exact commit SHA from the PR to prevent TOCTOU attacks

### PR Validation (`pr-validation.yml`)

**Purpose**: Validates pull request titles and descriptions follow project conventions.

**Triggers**:
- Pull request opened, edited, or synchronized

**Checks**:

1. **Title Validation**:
   - Must follow [Conventional Commits](https://www.conventionalcommits.org/) format
   - Required format: `type(scope): description`
   - Allowed types: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`, `revert`
   - Allowed scopes: `csharp`, `ci`, `docs`
   - Subject must start with lowercase letter
   - Use `!` suffix for breaking changes (e.g., `feat(csharp)!: breaking change`)

2. **Description Validation**:
   - Warns if missing issue reference (`Closes #NNN` or `Fixes #NNN`)
   - Fails if contains @ mentions (to avoid commit message pollution)

**Examples of valid PR titles**:
- `feat(csharp): add CloudFetch prefetching support`
- `fix(csharp): handle token expiration correctly`
- `docs(csharp): update OAuth configuration guide`
- `fix!(csharp): change default batch size to 2M rows`

### Pre-commit Checks (`pre-commit.yml`)

**Purpose**: Runs code quality and formatting checks using pre-commit hooks.

**Triggers**:
- Push to `main` or `maint-*` branches
- Pull requests (including from forks) to `main` or `maint-*` branches

**Security**: Uses `pull_request_target` with safe checkout to support fork PRs in private repositories.

**Checks**:
- File formatting (trailing whitespace, end-of-file, line endings)
- Large file detection
- Private key detection
- YAML/XML validation
- Spell checking (via codespell)
- Markdown formatting

**Local Setup**:
```bash
# Install pre-commit
pip install pre-commit

# Install hooks
pre-commit install

# Run manually
pre-commit run --all-files
```

## Dependabot

The `.github/dependabot.yml` configuration keeps dependencies up to date:
- **GitHub Actions**: Weekly updates for workflow actions
- **NuGet Packages**: Weekly updates for C# dependencies

Dependabot PRs will automatically follow the Conventional Commits format with appropriate prefixes.

## Adding New Workflows

When adding new workflows:

1. Follow the existing structure and naming conventions
2. Include the Apache License header
3. Use appropriate concurrency groups to cancel redundant runs
4. Set reasonable timeouts
5. Use proper permission scopes (principle of least privilege)
6. Add documentation in this README

## Troubleshooting

### Workflow not running

- Check that your changes touch the paths specified in the workflow's `paths` filter
- Ensure your PR title doesn't contain "WIP" (C# workflow skips WIP PRs)
- Verify you're targeting the correct branch (`main` or `maint-*`)

### Pre-commit checks failing

- Run `pre-commit run --all-files` locally to see failures
- Ensure all changes are committed (pre-commit only checks committed files)
- Check that file permissions are correct (`chmod +x` for shell scripts)

### PR validation failing

- Verify your PR title follows the Conventional Commits format: `type(scope): description`
- Remove any @ mentions from the PR description
- Ensure subject starts with lowercase letter
- Check that scope is one of: `csharp`, `ci`, `docs`
