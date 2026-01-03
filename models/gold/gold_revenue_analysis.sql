{{config(materialized='table',database='GOLD')}}

with orders as (
    select * from {{ ref('silver_orders') }}
    where is_completed
),

payments as (
    select * from {{ ref('silver_payments') }}
    where is_successful_payment
),

monthly_orders as (
    select
        order_year,
        order_month,
        order_year_month,
        order_quarter,
        order_year_quarter,
        count(distinct order_key) as total_orders,
        count(distinct customer_key) as unique_customers,
        sum(order_amount) as total_revenue,
        avg(order_amount) as avg_order_value,
        max(order_amount) as max_order_value,
        min(order_amount) as min_order_value,
        round(
            count(distinct order_key) * 1.0 / 
            nullif(count(distinct customer_key), 0),
            2
        ) as orders_per_customer
    from orders
    group by 1, 2, 3, 4, 5
),

monthly_payment_methods as (
    select
        to_char(o.order_date, 'YYYY-MM') as order_year_month,
        p.payment_method,
        count(distinct p.payment_key) as payment_count,
        sum(p.payment_amount) as payment_amount
    from payments p
    join orders o on p.order_key = o.order_key
    group by 1, 2
),

payment_pivot as (
    select
        order_year_month,
        sum(case when payment_method = 'credit_card' then payment_amount else 0 end) as credit_card_revenue,
        sum(case when payment_method = 'bank_transfer' then payment_amount else 0 end) as bank_transfer_revenue,
        sum(case when payment_method = 'paypal' then payment_amount else 0 end) as paypal_revenue,
        sum(case when payment_method = 'cash' then payment_amount else 0 end) as cash_revenue,
        sum(case when payment_method = 'other' then payment_amount else 0 end) as other_revenue
    from monthly_payment_methods
    group by 1
),

enriched as (
    select
        mo.*,
        lag(mo.total_revenue, 1) over (order by mo.order_year_month) as prev_month_revenue,
        lag(mo.total_revenue, 12) over (order by mo.order_year_month) as year_ago_revenue,
        lag(mo.total_orders, 1) over (order by mo.order_year_month) as prev_month_orders,
        round(
            (mo.total_revenue - lag(mo.total_revenue, 1) over (order by mo.order_year_month)) * 100.0 /
            nullif(lag(mo.total_revenue, 1) over (order by mo.order_year_month), 0),
            2
        ) as revenue_growth_mom_pct,
        round(
            (mo.total_orders - lag(mo.total_orders, 1) over (order by mo.order_year_month)) * 100.0 /
            nullif(lag(mo.total_orders, 1) over (order by mo.order_year_month), 0),
            2
        ) as orders_growth_mom_pct,
        round(
            (mo.total_revenue - lag(mo.total_revenue, 12) over (order by mo.order_year_month)) * 100.0 /
            nullif(lag(mo.total_revenue, 12) over (order by mo.order_year_month), 0),
            2
        ) as revenue_growth_yoy_pct,
        sum(mo.total_revenue) over (
            partition by mo.order_year
            order by mo.order_month
            rows between unbounded preceding and current row
        ) as ytd_revenue,
        sum(mo.total_orders) over (
            partition by mo.order_year
            order by mo.order_month
            rows between unbounded preceding and current row
        ) as ytd_orders,
        pp.credit_card_revenue,
        pp.bank_transfer_revenue,
        pp.paypal_revenue,
        pp.cash_revenue,
        pp.other_revenue,
        current_timestamp() as _updated_at
    from monthly_orders mo
    left join payment_pivot pp on mo.order_year_month = pp.order_year_month
)

select * from enriched
order by order_year_month desc