{{config(materialized='table',database='GOLD')}}

with customers as (
    select * from {{ ref('silver_customers') }}
),

orders as (
    select * from {{ ref('silver_orders') }}
),

payments as (
    select * from {{ ref('silver_payments') }}
),

customer_orders as (
    select
        o.customer_key,
        count(distinct o.order_key) as total_orders,
        count(distinct case when o.is_completed then o.order_key end) as completed_orders,
        count(distinct case when o.is_cancelled then o.order_key end) as cancelled_orders,
        count(distinct case when o.is_pending then o.order_key end) as pending_orders,
        min(o.order_date) as first_order_date,
        max(o.order_date) as last_order_date,
        datediff(day, min(o.order_date), max(o.order_date)) as customer_lifetime_days,
        sum(case when o.is_completed then o.order_amount else 0 end) as total_revenue,
        avg(case when o.is_completed then o.order_amount end) as avg_order_value,
        max(o.order_amount) as max_order_value,
        min(case when o.is_completed then o.order_amount end) as min_order_value,
        count(distinct o.order_year_month) as active_months
    from orders o
    group by 1
),

customer_payments as (
    select
        o.customer_key,
        count(distinct p.payment_key) as total_payments,
        count(distinct p.payment_method) as unique_payment_methods,
        sum(p.payment_amount) as total_paid,
        mode(p.payment_method) as preferred_payment_method
    from payments p
    join orders o on p.order_key = o.order_key
    where p.is_successful_payment
    group by 1
),

final as (
    select
        c.customer_key,
        c.customer_id,
        c.full_name,
        c.first_name,
        c.last_name,
        c.email,
        c.created_at as customer_since,
        coalesce(co.total_orders, 0) as total_orders,
        coalesce(co.completed_orders, 0) as completed_orders,
        coalesce(co.cancelled_orders, 0) as cancelled_orders,
        coalesce(co.pending_orders, 0) as pending_orders,
        coalesce(co.total_revenue, 0) as lifetime_value,
        coalesce(co.avg_order_value, 0) as avg_order_value,
        coalesce(co.max_order_value, 0) as max_order_value,
        coalesce(co.min_order_value, 0) as min_order_value,
        coalesce(cp.total_payments, 0) as total_payments,
        coalesce(cp.unique_payment_methods, 0) as unique_payment_methods,
        coalesce(cp.total_paid, 0) as total_paid_amount,
        cp.preferred_payment_method,
        co.first_order_date,
        co.last_order_date,
        coalesce(co.customer_lifetime_days, 0) as customer_lifetime_days,
        coalesce(co.active_months, 0) as active_months,
        datediff(day, co.last_order_date, current_date()) as days_since_last_order,
        case 
            when coalesce(co.total_revenue, 0) >= 500 then 'High Value'
            when coalesce(co.total_revenue, 0) >= 200 then 'Medium Value'
            when coalesce(co.total_revenue, 0) > 0 then 'Low Value'
            else 'No Revenue'
        end as customer_segment,
        case 
            when co.last_order_date is null then 'Never Ordered'
            when datediff(day, co.last_order_date, current_date()) <= 30 then 'Active'
            when datediff(day, co.last_order_date, current_date()) <= 90 then 'At Risk'
            when datediff(day, co.last_order_date, current_date()) <= 180 then 'Inactive'
            else 'Churned'
        end as customer_status,
        case 
            when datediff(day, co.last_order_date, current_date()) <= 30 then 3
            when datediff(day, co.last_order_date, current_date()) <= 90 then 2
            else 1
        end as recency_score,
        case 
            when coalesce(co.total_orders, 0) >= 5 then 3
            when coalesce(co.total_orders, 0) >= 2 then 2
            else 1
        end as frequency_score,
        case 
            when coalesce(co.total_revenue, 0) >= 500 then 3
            when coalesce(co.total_revenue, 0) >= 200 then 2
            else 1
        end as monetary_score,
        current_timestamp() as _updated_at
    from customers c
    left join customer_orders co on c.customer_key = co.customer_key
    left join customer_payments cp on c.customer_key = cp.customer_key
)

select * from final