# Test Infrastructure

This directory contains shared test infrastructure that can be used across multiple driver implementations (C#, Java, C++, Go).

## Contents

### `proxy-server/`

A standalone Thrift proxy server that enables testing of failure scenarios:
- Expired CloudFetch links
- Network timeouts
- Corrupted responses
- Connection failures

**Implementation**: Go (for portability and performance)
**Usage**: All driver implementations connect to this proxy during testing

See [proxy-server/README.md](./proxy-server/README.md) for details.

## Design Philosophy

This infrastructure is designed to be:
- **Language-agnostic**: Works with any driver implementation
- **Extractable**: Can be moved to a common repository
- **Standalone**: Runs independently of any specific driver
- **Configuration-driven**: Behavior controlled via YAML/JSON config

## Future Components

Additional infrastructure may include:
- Mock cloud storage service (for CloudFetch testing)
- Test data generators
- Result validators
- Performance profiling tools

## Status

- [x] Directory structure created
- [ ] Proxy server implementation
- [ ] Documentation
- [ ] CI integration
