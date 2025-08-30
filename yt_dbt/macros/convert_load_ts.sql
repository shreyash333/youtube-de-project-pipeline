{% macro convert_load_ts(epoch_column) %}
    -- This macro converts epoch timestamps (in microseconds) into a human-readable datetime string.
    -- Arguments:
    --   epoch_column: Column containing epoch timestamps in microseconds (e.g., 1692871234567890).
    -- Process:
    --   1. Divide by 1e6 → convert microseconds to seconds.
    --   2. Round to 2 decimals for precision handling.
    --   3. Convert to TIMESTAMP_NTZ (no timezone).
    --   4. Format as 'YYYY-MM-DD HH24:MI:SS' string.
    -- Returns:
    --   A VARCHAR column representing the formatted datetime.

    TO_CHAR(
        TO_TIMESTAMP_NTZ(
            ROUND("{{ epoch_column }}" / 1e6, 2)  -- Convert µs → s, round
        ),
        'YYYY-MM-DD HH24:MI:SS'  -- Final datetime string format
    )
{% endmacro %}
