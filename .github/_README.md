<!-- GitHub will publish this readme on the main repo page if the name is `README.md` so we've added the leading underscore to prevent this -->
<!-- Do not rename this file `README.md` -->
<!-- See https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-readmes -->

## What are GitHub Actions?

GitHub Actions are used for many different purposes.  We use them to run tests in CI, validate PRs are in an expected state, and automate processes.

- [Overview of GitHub Actions](https://docs.github.com/en/actions/learn-github-actions/understanding-github-actions)
- [What's a workflow?](https://docs.github.com/en/actions/using-workflows/about-workflows)
- [GitHub Actions guides](https://docs.github.com/en/actions/guides)

___

## General Standards

For detailed standards on permissions, secrets, triggers, formatting, and best practices, see the [dbt-core GitHub Actions README](https://github.com/dbt-labs/dbt-core/blob/main/.github/_README.md).

___

### Reusable Workflows

Workflows prefixed with `_` (e.g., `_run-tests.yml`, `_unit-tests.yml`) are reusable workflows meant to be called by other workflows. They use the `workflow_call` trigger and can accept inputs and secrets. These workflows encapsulate common patterns and can be reused across multiple workflows to avoid duplication.

Example usage:
```yaml
jobs:
  unit-tests:
    uses: ./.github/workflows/_unit-tests.yml
    with:
      package: ${{ matrix.package }}
      branch: ${{ inputs.branch }}
```

### Actions

Actions are reusable pieces of code that can be called from workflows. They live in `.github/actions` and contain common operations we perform frequently. Actions can be composite actions (YAML-based) or JavaScript actions.

Example usage:
```yaml
steps:
  - uses: ./.github/actions/bot-commit
    with:
      message: "Bump version from ${{ steps.version.outputs.initial }} to ${{ steps.version.outputs.final }}"
```
