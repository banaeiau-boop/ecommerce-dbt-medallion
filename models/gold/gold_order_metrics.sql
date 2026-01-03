{{config(materialized='table',database='GOLD')}}

with orders as (
    select * from {{ ref('silver_orders') }}
),

daily_metrics as (
    select
        order_date,
        order_year,
        order_month,
        order_day,
        order_quarter,
        order_year_month,
        order_year_quarter,
        order_day_name,
        count(distinct order_key) as total_orders,
        count(distinct case when is_completed then order_key end) as completed_orders,
        count(distinct case when is_cancelled then order_key end) as cancelled_orders,
        count(distinct case when is_pending then order_key end) as pending_orders,
        count(distinct customer_key) as unique_customers,
        sum(case when is_completed then order_amount else 0 end) as total_revenue,
        avg(case when is_completed then order_amount end) as avg_order_value,
        max(order_amount) as max_order_value,
        min(case when is_completed then order_amount end) as min_order_value,
        sum(order_amount) as potential_revenue,
        sum(case when is_cancelled then order_amount else 0 end) as lost_revenue,
        round(
            count(distinct case when is_completed then order_key end) * 100.0 / 
            nullif(count(distinct order_key), 0), 
            2
        ) as completion_rate,
        round(
            count(distinct case when is_cancelled then order_key end) * 100.0 / 
            nullif(count(distinct order_key), 0), 
            2
        ) as cancellation_rate,
        round(
            count(distinct order_key) * 1.0 / 
            nullif(count(distinct customer_key), 0),
            2
        ) as orders_per_customer
    from orders
    group by order_date, order_year, order_month, order_day, order_quarter, order_year_month, order_year_quarter, order_day_name
),

final as (
    select
        order_date,
        order_year,
        order_month,
        order_day,
        order_quarter,
        order_year_month,
        order_year_quarter,
        order_day_name,
        total_orders,
        completed_orders,
        cancelled_orders,
        pending_orders,
        unique_customers,
        total_revenue,
        avg_order_value,
        max_order_value,
        min_order_value,
        potential_revenue,
        lost_revenue,
        completion_rate,
        cancellation_rate,
        orders_per_customer,
        sum(total_revenue) over (
            order by order_date 
            rows between unbounded preceding and current row
        ) as cumulative_revenue,
        sum(total_orders) over (
            order by order_date 
            rows between unbounded preceding and current row
        ) as cumulative_orders,
        avg(total_revenue) over (
            order by order_date 
            rows between 6 preceding and current row
        ) as revenue_7day_ma,
        avg(total_orders) over (
            order by order_date 
            rows between 6 preceding and current row
        ) as orders_7day_ma,
        lag(total_revenue, 1) over (order by order_date) as prior_day_revenue,
        lag(total_revenue, 7) over (order by order_date) as week_ago_revenue,
        round(
            (total_revenue - lag(total_revenue, 1) over (order by order_date)) * 100.0 /
            nullif(lag(total_revenue, 1) over (order by order_date), 0),
            2
        ) as revenue_growth_vs_prior_day_pct,
        round(
            (total_revenue - lag(total_revenue, 7) over (order by order_date)) * 100.0 /
            nullif(lag(total_revenue, 7) over (order by order_date), 0),
            2
        ) as revenue_growth_vs_week_ago_pct,
        current_timestamp() as _updated_at
    from daily_metrics
)

select * from final
order by order_date desc