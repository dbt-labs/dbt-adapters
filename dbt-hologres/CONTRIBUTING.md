# Contributing to dbt-hologres

We welcome contributions to `dbt-hologres`! This document outlines the process for contributing to this adapter.

## Development Setup

### Prerequisites

- Python >= 3.10
- Git
- Hologres instance for testing (or use the provided test credentials)

### Setting Up Your Development Environment

1. Clone the repository:
```bash
git clone https://github.com/dbt-labs/dbt-adapters.git
cd dbt-adapters/dbt-hologres
```

2. Install dependencies using hatch:
```bash
hatch env create
```

3. Set up pre-commit hooks:
```bash
hatch run setup
```

4. Configure test environment:
```bash
cp test.env.example test.env
# Edit test.env with your Hologres connection details
```

## Development Workflow

### Running Tests

Run unit tests:
```bash
hatch run unit-tests
```

Run integration tests (requires Hologres connection):
```bash
hatch run integration-tests
```

Run specific tests:
```bash
hatch run unit-tests tests/unit/test_connection.py
```

### Code Quality

Run code quality checks:
```bash
hatch run code-quality
```

This runs:
- black (code formatting)
- flake8 (linting)
- mypy (type checking)
- isort (import sorting)

### Making Changes

1. Create a new branch for your feature or bugfix:
```bash
git checkout -b feature/your-feature-name
```

2. Make your changes and add tests

3. Run tests and code quality checks:
```bash
hatch run unit-tests
hatch run code-quality
```

4. Commit your changes:
```bash
git add .
git commit -m "Description of your changes"
```

5. Push to your fork and submit a pull request

## Code Structure

```
dbt-hologres/
├── src/dbt/adapters/hologres/     # Adapter implementation
│   ├── connections.py              # Connection management
│   ├── impl.py                     # Adapter logic
│   ├── column.py                   # Column type handling
│   ├── relation.py                 # Relation objects
│   └── relation_configs/           # Configuration classes
├── src/dbt/include/hologres/       # SQL macros and config
│   ├── macros/                     # SQL macro definitions
│   ├── dbt_project.yml
│   └── profile_template.yml
└── tests/                          # Test suite
    ├── unit/                       # Unit tests
    └── functional/                 # Integration tests
```

## Testing Guidelines

### Unit Tests

- Test individual functions and classes in isolation
- Mock external dependencies (database connections, etc.)
- Use pytest fixtures for common test setup
- Aim for >80% code coverage

### Integration Tests

- Test actual database interactions
- Use the test Hologres instance
- Clean up test data after each test
- Test both success and failure scenarios

### Writing New Tests

1. Create test files in `tests/unit/` or `tests/functional/`
2. Follow naming convention: `test_*.py`
3. Use descriptive test names: `test_connection_with_ssl_disabled`
4. Add docstrings explaining what each test verifies

## Hologres-Specific Considerations

### Dynamic Tables

When working with Dynamic Table features:
- Verify SQL syntax against Hologres V3.1+ documentation
- Test both logical and physical partition configurations
- Validate freshness and refresh mode settings
- Test with different computing resources (serverless, local)

### Psycopg3 Migration

This adapter uses Psycopg 3 instead of Psycopg 2:
- Use `psycopg` module, not `psycopg2`
- Connection API differences (e.g., `info.backend_pid` vs `get_backend_pid()`)
- Type handling changes
- See [Psycopg 3 documentation](https://www.psycopg.org/psycopg3/docs/) for details

### Connection Configuration

- Default port is 80, not 5432
- SSL is disabled by default
- Username format: `BASIC$username`
- Case-sensitive credentials

## Pull Request Guidelines

### Before Submitting

- [ ] Tests pass locally
- [ ] Code quality checks pass
- [ ] Documentation updated (if applicable)
- [ ] CHANGELOG entry added (for user-facing changes)
- [ ] Commit messages are clear and descriptive

### PR Description

Include in your PR description:
- Summary of changes
- Motivation and context
- Related issues (if any)
- Testing performed
- Breaking changes (if any)

### Review Process

1. Automated checks will run on your PR
2. Maintainers will review your code
3. Address any feedback
4. Once approved, a maintainer will merge your PR

## Reporting Issues

When reporting issues:
- Use GitHub Issues
- Include adapter version
- Include dbt-core version
- Provide minimal reproduction steps
- Include relevant error messages
- Specify Hologres version if relevant

## Documentation

Update documentation for:
- New features
- API changes
- Configuration options
- Hologres-specific behavior

Documentation is in:
- README.md (user-facing)
- Code comments and docstrings (developer-facing)
- This CONTRIBUTING.md (contributor-facing)

## Getting Help

- [dbt Community Slack](https://www.getdbt.com/community/)
- [GitHub Discussions](https://github.com/dbt-labs/dbt-adapters/discussions)
- [GitHub Issues](https://github.com/dbt-labs/dbt-adapters/issues)

## Code of Conduct

Please note that this project is released with a Contributor Code of Conduct. By participating in this project you agree to abide by its terms.

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.

Thank you for contributing to dbt-hologres!
