{% macro quote_identifier(column_name) %}
    -- This macro safely quotes a column or identifier.
    -- Arguments:
    --   column_name: Name of the column or identifier.
    -- Purpose:
    --   Ensures column names with special characters, spaces, or reserved keywords 
    --   are properly wrapped in double quotes for Snowflake SQL.
    -- Example:
    --   {{ quote_identifier('user') }} → "user"
    
    "{{ column_name }}"
{% endmacro %}
