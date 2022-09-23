
SELECT 
--packages
{{ dbt_utils.surrogate_key(['o.order_id', 'c.customerid', 'p.productid']) }} as sk_orders,

o.order_id, o.order_date, o.shipdate, o.shipmode, 
       o.ordercostprice, o.ordersellingprice,
       o.ordersellingprice - o.ordercostprice orderprofit,       
       --from raw customer
       c.customerid, c.customername, c.segment, c.country,
       --from Product
       p.productid, p.category, p.productname, p.subcategory,
      --macro
       o.ordersellingprice - o.ordercostprice orderprofit,       
      {{ markup('ordersellingprice','ordercostprice') }} as markup

  FROM {{ref('raw_orders')}} as o
LEFT JOIN {{ref('raw_customer')}} as c
on o.customerid = c.customerid
LEFT JOIN {{ref('raw_product')}} as p
on o.productid = p.productid