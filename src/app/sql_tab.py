import re

import duckdb
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
    # We check the start because CTEs (WITH...) can contain these words safely as aliases
    for word in forbidden:
        if q.startswith(word) or f" {word} " in q:
            return False
    return True


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
                "Columns": ", ".join(df_ref.columns),
            }
        )
    st.table(schema_info)

    # SQL Input Area
    st.markdown("#### Run your Query")
    query = st.text_area("SQL Editor", placeholder="Enter a query", height=150)

    if st.button("Execute SQL"):
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
                con.register(sql_name, data_dict[orig_name])

            # Execute and convert back to DataFrame
            result = con.execute(query).df()

            st.success(f"Returned {len(result)} rows")
            st.dataframe(result, width="stretch")

        except Exception as e:
            st.error(f"SQL Error: {e}")
