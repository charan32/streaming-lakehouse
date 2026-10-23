-- Fewer sessions can pay than order, and fewer can order than add to cart.
select *
from {{ ref('fct_daily_kpis') }}
where conversion_rate > order_rate or order_rate > cart_rate
