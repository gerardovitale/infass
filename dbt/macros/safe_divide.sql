{% macro divide(numerator, denominator) %}
    {% if target.type == 'bigquery' %}
        SAFE_DIVIDE({{ numerator }}, {{ denominator }})
    {% else %}
        CASE
            WHEN {{ denominator }} = 0 THEN NULL
            ELSE CAST({{ numerator }} AS DOUBLE) / {{ denominator }}
        END
    {% endif %}
{% endmacro %}
