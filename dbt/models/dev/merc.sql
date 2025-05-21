{{ config(
    materialized='table',
    enabled=(target.name == 'dev'),
    project='inflation-assistant'
) }}

SELECT *
FROM `inflation-assistant.infass.merc`
WHERE
    date BETWEEN '2025-04-01' AND '2025-04-30'
    AND name IN (
        'aceite de oliva virgen extra hacendado',
        'cerveza especial estrella galicia'
    )
