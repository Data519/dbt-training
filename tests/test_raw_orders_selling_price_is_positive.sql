with orders as (
    select * from {{ ref('raw_orders') }}
)
select order_id, sum(ordersellingprice) as total_sp
  from orders 
  group by order_id
  having total_sp < 0