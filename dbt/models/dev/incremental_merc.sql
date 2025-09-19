{{ config(
    materialized='table',
    enabled=(target.name in ['dev','test']),
    project='inflation-assistant'
) }}

{% set end_date %} CURRENT_DATE() {% endset %}
{% set start_date %} DATE_SUB({{ end_date }}, INTERVAL 30 DAY) {% endset %}

SELECT
    *
FROM `inflation-assistant.infass.incremental_merc`
WHERE
    date between {{ start_date }} and {{ end_date }}
    AND name IN (
        'aceite de oliva virgen extra hacendado',
        'cerveza especial estrella galicia',
        'obleas para helado hacendado'
    )
