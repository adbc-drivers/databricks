# Thrift Protocol Test Suite

## Overview

This directory contains the design and specifications for a comprehensive test suite that validates ADBC driver implementations against the Databricks Thrift protocol.

**Status**: Design Phase
**Initial Target**: C# ADBC Driver (this repository)
**Future Targets**: Java (JDBC), C++ (ODBC), Go (ADBC) drivers

## Quick Links

- **[Design Document](./design.md)** - Full design, architecture, and implementation plan
- **[Test Specifications](./specs/)** - Detailed test case specifications (to be created)

## What's in This Test Suite?

### 300 Test Cases Across 16 Categories

| Category | Tests | Priority | Description |
|----------|-------|----------|-------------|
| Session Lifecycle | 15 | Critical | OpenSession, CloseSession, timeouts |
| Statement Execution | 25 | Critical | Sync/async execution, cancellation |
| Metadata Operations | 40 | High | GetCatalogs, GetSchemas, GetTables, etc. |
| Arrow Format | 20 | High | Arrow IPC, compression, type handling |
| CloudFetch | 20 | Critical | Cloud storage results, link expiration |
| Direct Results | 15 | High | TSparkDirectResults optimization |
| Parameterized Queries | 20 | High | Named/positional parameters |
| Result Fetching | 15 | High | Pagination, cursor management |
| Error Handling | 30 | Critical | Error codes, recovery, retries |
| Timeout & Cleanup | 12 | Medium | Session/operation timeouts |
| Concurrency | 15 | Medium | Thread safety, parallel operations |
| Protocol Versions | 12 | Medium | Version negotiation, compatibility |
| Security | 15 | High | Authentication, authorization |
| Performance | 10 | Low | Limits, batch sizes |
| Edge Cases | 36 | Medium | NULL handling, empty results, etc. |

**Total: ~300 test cases**

## Architecture

```
┌─────────────────────────────────────────┐
│  Test Specifications (Markdown)         │
│  Language-agnostic test definitions     │
└─────────────────────────────────────────┘
                 ↓
┌─────────────────────────────────────────┐
│  Standalone Proxy Server (Go)           │
│  Failure injection, request interception│
└─────────────────────────────────────────┘
                 ↓
┌─────────────┬─────────────┬─────────────┐
│  C# Tests   │ Java Tests  │  C++ Tests  │
│  (Initial)  │  (Future)   │  (Future)   │
└─────────────┴─────────────┴─────────────┘
```

### Key Components

1. **Test Specifications** (`specs/` directory)
   - Language-agnostic test case definitions
   - Clear test steps and expected behavior
   - Shared by all driver implementations

2. **Proxy Server** (`../../test-infrastructure/proxy-server/`)
   - Standalone Go application
   - Intercepts and modifies Thrift requests/responses
   - Injects failures for testing error scenarios
   - Used by all language implementations

3. **C# Test Implementation** (`../../csharp/test/ThriftProtocol/`)
   - xUnit-based test suite
   - Follows test specifications
   - Initial reference implementation

## Why This Design?

### Language-Agnostic

Different drivers use different languages:
- **C# ADBC**: This repository
- **Java JDBC**: Separate repository
- **C++ ODBC**: Separate repository
- **Go ADBC**: Part of Apache Arrow ADBC

By keeping specifications language-agnostic, we ensure:
- ✅ Consistent behavior across all drivers
- ✅ Shared understanding of Thrift protocol
- ✅ Easier review and maintenance
- ✅ Single source of truth for protocol compliance

### Standalone Proxy Server

Testing failure scenarios (expired CloudFetch links, network timeouts, etc.) requires:
- Intercepting real Thrift traffic
- Modifying responses on-the-fly
- Injecting controlled failures

A standalone proxy server:
- ✅ Works with all programming languages
- ✅ Single implementation to maintain
- ✅ Configuration-driven (no code changes)
- ✅ Easy to run in CI/CD

### Extractable Design

This test infrastructure is designed to be extracted to a common repository:

**Now**:
```
adbc-drivers/databricks/
├── docs/designs/thrift-protocol-tests/
└── test-infrastructure/proxy-server/
```

**Future**:
```
github.com/databricks/thrift-test-infrastructure/
├── specs/              # Test specifications
├── proxy-server/       # Standalone proxy
└── examples/
    ├── csharp/
    ├── java/
    └── cpp/
```

## Getting Started

### For Reviewers

1. **Read the [Design Document](./design.md)**
   - Understand the overall approach
   - Review architecture decisions
   - Provide feedback on multi-language strategy

2. **Review Test Categories**
   - Are all important scenarios covered?
   - Are priorities correct?
   - Any missing test cases?

3. **Consider Implementation**
   - Is the C# implementation approach sound?
   - Will this work for Java/C++/Go drivers?
   - Is the proxy server design adequate?

### For Implementers (Future)

1. **Read Test Specifications**
   - Each spec describes test steps clearly
   - Expected behavior is defined
   - Implementation notes provided

2. **Set Up Proxy Server**
   ```bash
   cd test-infrastructure/proxy-server
   go run main.go --config proxy-config.yaml
   ```

3. **Implement Tests in Your Language**
   - Follow test specifications
   - Use your language's test framework
   - Connect to proxy for failure testing

## Implementation Status

- [x] Design document
- [ ] Test specifications (16 documents)
- [ ] Proxy server implementation
- [ ] C# test implementation
- [ ] Java test implementation (future)
- [ ] C++ test implementation (future)
- [ ] Go test implementation (future)

## Timeline

- **Weeks 1-2**: Write specifications, implement proxy server
- **Weeks 3-4**: C# critical tests (Session, CloudFetch, Execution)
- **Weeks 5-6**: C# metadata and Arrow tests
- **Weeks 7-8**: C# advanced features (parameters, errors)
- **Weeks 9-10**: C# robustness tests (concurrency, edge cases)
- **Future**: Adapt for Java, C++, Go drivers

## Questions?

For questions about this design:
- Review the [Design Document](./design.md)
- Check the [original design doc](https://github.com/databricks-eng/universe/blob/master/peco/docs/designs/adbc-thrift-test-suite-design.md)
- Reach out to the PECO team

## Contributing

When contributing test specifications:
1. Follow the template in the design doc
2. Keep specifications language-agnostic
3. Include clear test steps and expected behavior
4. Add implementation notes for tricky cases
5. Consider all target languages (C#, Java, C++, Go)
