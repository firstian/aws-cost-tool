# AGENTS.md

## Purpose
- Guide agentic coding tools working in this repository.
- Follow existing conventions in `src/aws_cost_tool` and `src/app`.
- Use uv for dependencies, builds, and test execution.

## Repository Layout
- `src/aws_cost_tool`: core library for Cost Explorer data access.
- `src/aws_cost_tool/services`: service usage categorization plugins.
- `src/app`: Streamlit UI, data sources, and report views.
- `tests/aws_cost_tool`: pytest unit tests for library modules.
- `tests/app`: Streamlit app tests (auto-skipped if Streamlit missing).
- `pyproject.toml`: packaging metadata and dependency config.

## Setup
- Python >= 3.14 (see `pyproject.toml`).
- Install deps: `uv sync`
- Install app extras: `uv sync --extra app`
- Dev deps live in `dependency-groups.dev` and are in `uv.lock`.

## Build / Run
- Package build: `uv build`
- Run CLI entrypoint: `uv run aws-cost-tool --profile=mock_data`
- Run app against file data: `uv run aws-cost-tool --data-dir /path/to/csvs`
- Pass tag key for tag breakdowns: `uv run aws-cost-tool --tag-key Environment`

## Tests
- Full suite: `uv run pytest`
- Single file: `uv run pytest tests/aws_cost_tool/cost_explorer_test.py`
- Single test: `uv run pytest tests/aws_cost_tool/cost_explorer_test.py::TestDateRange::test_init_with_dates`
- By keyword: `uv run pytest -k "DateRange"`
- Coverage (optional): `uv run pytest --cov=aws_cost_tool --cov-report=term-missing`
- App tests skip if `streamlit` is not installed (see `tests/conftest.py`).

## Lint / Format / Type Check
- No linter or formatter configured in `pyproject.toml`.
- No type checker configured (mypy/pyright not wired).
- Follow existing formatting and typing style; avoid large reformat passes.

## Import Style
- Group imports: standard library, third-party, local, separated by blank lines.
- Prefer explicit imports over `*` and keep them sorted per group.
- Use module aliases when they improve readability (`import app.ui_components as ui`).
- Keep type-only imports inline; no `TYPE_CHECKING` pattern is used.

## Formatting
- 4-space indentation, no tabs.
- Keep line length reasonable (roughly <= 100) and wrap long call arguments.
- Use trailing commas in multiline literals and argument lists.
- Keep blank lines between top-level defs and class blocks.
- Comments are minimal; prefer clear naming and docstrings.

## Types
- Type hints are expected for public functions, protocols, and dataclasses.
- Use Python 3.14 syntax: `|` unions and `type` aliases.
- Prefer `list[str]`, `dict[str, Any]`, `Sequence[str]` over typing generics.
- Use `dataclass(frozen=True, kw_only=True)` for immutable value objects.
- In tests, use `# type: ignore` only when unavoidable.

## Naming
- Functions/variables: `snake_case`.
- Classes/enums: `PascalCase`.
- Constants: `UPPER_SNAKE_CASE`.
- Tests: `*_test.py`, classes named `TestX`, methods named `test_*`.

## Error Handling
- Prefer specific exceptions; avoid broad `except`.
- Log recoverable errors and return safe defaults when appropriate.
- Re-raise unexpected AWS errors rather than swallowing them.
- Validation raises `ValueError`/`TypeError` with helpful messages.
- Avoid catching broad exceptions in Streamlit flows unless required for `st.rerun`.

## Logging
- Use module-level `logger = logging.getLogger(__name__)`.
- Use `logger.info` for operational events, `logger.error` for failures.
- Streamlit app configures logging once in `src/app/main.py`.

## DataFrame Conventions
- Prefer constructing DataFrames from lists of dicts for clarity.
- Normalize column names with clear capitalization (`StartDate`, `EndDate`, etc.).
- Use `pd.concat` for paginated results; return empty DataFrames with columns.
- After `groupby`, call `reset_index(drop=True)` when consumed downstream.
- Be explicit when pivoting (see `pivot_data`).

## Streamlit UI Patterns
- Use `st.cache_resource` for expensive shared data (service loading).
- Use `st.cache_data` with TTL for lightweight checks.
- Store UI state in `st.session_state` and guard missing keys.
- Use helper components in `app.ui_components` for consistency.
- Avoid heavy work outside of `st.spinner` in fetch flows.

## AWS / Boto3 Usage
- Create clients via `create_ce_client`; it refreshes SSO credentials when needed.
- Handle auth issues via `check_aws_auth` and `refresh_credentials`.
- Be mindful of Cost Explorer rate limits; delay with `time.sleep`.
- Filter out AWS-managed tags (`aws:` prefix) when presenting user tags.

## Service Plugin Patterns
- Service plugins live in `src/aws_cost_tool/services`.
- Subclass `ServiceBase` and implement `name`, `shortname`, `categorize_usage`.
- Use `categorize_usage_costs` to assemble category/subtype rollups.
- Keep extractor functions pure: filter, copy, and return DataFrames.
- Keep `slugify_name` for file-safe prefixes.

## Testing Conventions
- Use `pytest` fixtures and `unittest.mock` for AWS/IO boundaries.
- Prefer `pytest.raises` with `match=` for error messages.
- Use `@patch` decorators for boto3, time, and AWS functions.
- Keep test data inline and readable; use small DataFrames.
- Prefer explicit asserts over snapshot-style comparisons.

## Documentation
- README documents setup and demo usage; update it if commands change.
- Docstrings are short and describe intent, not implementation details.

## Cursor / Copilot Rules
- No Cursor rules found in `.cursor/rules/` or `.cursorrules`.
- No Copilot instructions found in `.github/copilot-instructions.md`.

## Agent Behavior Notes
- Do not introduce new tooling without updating `pyproject.toml`.
- If you add dependencies, update `uv.lock` via `uv sync`.
- Avoid large mechanical refactors unless explicitly requested.
- Prefer small, focused changes with tests.
- Keep new code consistent with existing patterns.
