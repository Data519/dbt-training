{% macro markup(ordersellingprice, ordercostprice) %}
(ordersellingprice - ordercostprice) / ordercostprice
{% endmacro%}