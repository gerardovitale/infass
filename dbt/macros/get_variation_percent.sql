{% macro get_variation_percent(latest, earliest) %}
ROUND(
     SAFE_DIVIDE({{ latest }} - {{ earliest }}, {{ earliest }}),
     {{ var('default_round_precision') }}
)
{% endmacro %}
