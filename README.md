# AWS Cost Tool

A grab bag of sample code that use boto3 to dig into Cost Explorer output.
Most of this is vibe coded with all the frontier models, mix-and-match and then
some manual touch up to tweak the overall design. The final code is manually
reviewed to ensure that I understand what is going on.

## Set up

After cloning the repo, run:

```bash
uv sync --extra app
uv run pytest
```

## Streamlit app

There is a small demo Streamlit app. To run it with mock data without needing
AWS access, just run it as:

```bash
aws-cost-tool --profile=mock_data
```
