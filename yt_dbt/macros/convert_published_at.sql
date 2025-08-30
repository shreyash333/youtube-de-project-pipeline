{% macro convert_published_at(published_at_col) %}
    -- This macro converts ISO 8601-style YouTube timestamps into a human-readable datetime string.
    -- Arguments:
    --   published_at_col: Column containing timestamp strings in format "YYYY-MM-DDTHH:MI:SSZ"
    -- Process:
    --   1. Parse the input string with TO_TIMESTAMP_NTZ using the ISO format mask:
    --        - "YYYY-MM-DD"T"HH24:MI:SS"Z"
    --          T and Z are treated as literal characters.
    --   2. Convert parsed TIMESTAMP_NTZ to a string in 'YYYY-MM-DD HH24:MI:SS' format.
    -- Returns:
    --   A VARCHAR column representing the formatted datetime.

    TO_CHAR(
        TO_TIMESTAMP_NTZ("{{ published_at_col }}", 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
        'YYYY-MM-DD HH24:MI:SS'  -- Standard readable datetime format
    )
{% endmacro %}
