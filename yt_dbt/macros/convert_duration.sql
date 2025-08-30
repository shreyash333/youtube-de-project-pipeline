{% macro convert_duration(duration_col) %}
    -- This macro converts YouTube video duration strings (ISO 8601 format) into total seconds.
    -- Example input: "PT1H2M30S" → Output: 3750 (1*3600 + 2*60 + 30)
    -- Arguments:
    --   duration_col: Column containing ISO 8601 duration strings (e.g., PT#H#M#S).
    -- Returns:
    --   An integer expression representing total duration in seconds.

    (
        -- Extract hours (H), multiply by 3600. If null, use 0.
        COALESCE(REGEXP_SUBSTR("{{ duration_col }}", '([0-9]+)H', 1, 1, 'e', 1)::INT, 0) * 3600

        -- Extract minutes (M), multiply by 60. If null, use 0.
        + COALESCE(REGEXP_SUBSTR("{{ duration_col }}", '([0-9]+)M', 1, 1, 'e', 1)::INT, 0) * 60

        -- Extract seconds (S). If null, use 0.
        + COALESCE(REGEXP_SUBSTR("{{ duration_col }}", '([0-9]+)S', 1, 1, 'e', 1)::INT, 0)
    )
{% endmacro %}
