{{ config(
    materialized='table',
    enabled=(target.name == 'dev'),
    project='inflation-assistant'
) }}
SELECT
    *
FROM `inflation-assistant.infass.merc`
WHERE
    date BETWEEN DATE_SUB({{ get_last_saturday_date() }}, INTERVAL '30' DAY) AND {{ get_last_saturday_date() }}
    AND name IN ('aceite de oliva virgen extra hacendado', 'cerveza especial estrella galicia', 'obleas para helado hacendado')
