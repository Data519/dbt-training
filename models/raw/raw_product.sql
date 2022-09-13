{{
    config(
        materialized='table'
    )
}}

SELECT *
  FROM raw.globalmart.product