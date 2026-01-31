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

There are the following configuration options:

- `profile` is the AWS profile
- `tag_key` is the AWS Tag Key used to select tags

Both of these can be configured in three different ways (in order of precedence):

1. Specified in the flag `--profile` and `--tag-key`
2. Set in environment variables `AWS_PROFILE` and `TAG_KEY`
3. In the config file `config.yml`

The config directory (defaults to `~/.config/aws-cost-tool`) can be overridden
by the flag `--config`.

### Custom reports

User can add their own SQL queries of the DataFrame fetched from Cost Explorer by
adding a `queries.yml` file in the config directory. The file is a list of queries
with the following format:

```yaml
my_custom_report:
  title: "My custom report"
  description: "An example custom report"
  sql: |
    SELECT * from cost_df
    LIMIT 5
    ORDER BY StartDate DESC;
```

If the file exists and is not empty, the SQL sandbox tab will display a drop down
with all the queries.
