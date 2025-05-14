{% macro get_last_saturday_date() %}
DATE_SUB(
    CURRENT_DATE(),
    INTERVAL MOD(EXTRACT(DAYOFWEEK FROM CURRENT_DATE()), 7) DAY
)
{% endmacro %}
