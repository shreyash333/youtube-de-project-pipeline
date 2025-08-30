{% macro split_to_rows(column_name) %}
    -- This macro splits a comma-separated string column into multiple rows.
    -- Arguments:
    --   column_name: Column containing comma-separated values.
    -- Process:
    --   1. Uses SPLIT() to break the string into an array.
    --   2. Uses LATERAL FLATTEN() to expand array elements into individual rows.
    -- Returns:
    --   A table alias `t` with one row per split value (accessible as t.value).
    
    LATERAL FLATTEN(input => SPLIT({{ column_name }}, ',')) AS t
{% endmacro %}
