import re

import duckdb
import pandas as pd
import streamlit as st


@st.cache_data
def sql_safe_name(key: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "_", key).lower().strip("_")


def is_query_safe(query: str) -> bool:
    # Convert to uppercase for consistent checking
    q = query.upper().strip()

    # List of forbidden "Writing" keywords
    forbidden = [
        "DROP",
        "DELETE",
        "UPDATE",
        "ALTER",
        "INSERT",
        "COPY",
        "INSTALL",
        "LOAD",
    ]

    # Check if the query starts with any forbidden command
    # We check the start because CTEs (WITH...) can contain these words safely as
    # aliases
    for word in forbidden:
        if q.startswith(word) or f" {word} " in q:
            return False
    return True


@st.cache_data
def get_column_names(df: pd.DataFrame) -> list[str]:
    # 1. Get names from the index (MultiIndex or Single)
    # Filter out 'None' for unnamed indices (like a standard RangeIndex)
    index_cols = [str(name) for name in df.index.names if name is not None]

    # 2. Get names from the columns
    # If columns are MultiIndex, flatten the names into strings
    if isinstance(df.columns, pd.MultiIndex):
        data_cols = ["_".join(map(str, c)) for c in df.columns]
    else:
        data_cols = [str(c) for c in df.columns]

    return index_cols + data_cols


def get_sql_ready_df(df: pd.DataFrame) -> pd.DataFrame:
    # Check if it's a MultiIndex OR if a single index has a name (e.g., 'Date')
    # Standard 'RangeIndex' has no name and is just 0, 1, 2...
    if isinstance(df.index, pd.MultiIndex) or df.index.name is not None:
        # Reset all the name levels and leave the last one.
        named_levels = [name for name in df.index.names[:-1] if name is not None]
        return df.reset_index(named_levels)

    return df


@st.fragment
def render_sql_sandbox():
    cost_df = st.session_state.cost_data.get("cost_df")
    if cost_df is None or cost_df.empty:
        st.write("No Data")
        return

    data_dict = st.session_state.cost_data
    # Create a mapping of "SQL-friendly name" -> "Original Key"
    table_mapping = {sql_safe_name(k): k for k in data_dict.keys()}

    # Display available tables at the top
    st.write("#### Available Tables")
    schema_info = []
    for sql_name, orig_name in table_mapping.items():
        df_ref = data_dict[orig_name]
        schema_info.append(
            {
                "SQL Table Name": f"`{sql_name}`",
                "Original Source": orig_name,
                "Rows": df_ref.shape[0],
                "Columns": ", ".join(get_column_names(df_ref)),
            }
        )
    st.table(schema_info)

    # SQL Input Area
    st.markdown("#### Run your Query")
    query = st.text_area(
        "SQL Editor", placeholder="Enter a query", key="sql_text", height=150
    )

    if st.button("Execute SQL", key="run_sql"):
        if query == "":
            st.warning("Enter a valid query")
            return

        if not is_query_safe(query):
            st.error("Only SELECT queries are allowed in this sandbox.")
            return

        try:
            # Create a private connection for this execution
            con = duckdb.connect(database=":memory:")

            # Register each dataframe into the connection
            for sql_name, orig_name in table_mapping.items():
                df = get_sql_ready_df(data_dict[orig_name])
                con.register(sql_name, df)

            # Execute and convert back to DataFrame
            result = con.execute(query).df()

            st.success(f"Returned {len(result)} rows")
            st.dataframe(result, width="stretch")

        except Exception as e:
            st.error(f"SQL Error: {e}")
