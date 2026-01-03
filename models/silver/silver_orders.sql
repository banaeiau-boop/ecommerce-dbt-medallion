{{config(materialized='table',database='SILVER')}}

/*
    Silver Layer - Orders
    
    Purpose: Clean and standardize order data with business rules
    Source: Bronze orders
    Transformations:
        - Generate surrogate keys
        - Join to customer dimension
        - Standardize order statuses
        - Add business logic flags
        - Extract date components
        - Validate amounts
    
    Grain: One row per order
    Quality: Only orders with valid customers and positive amounts
*/

with bronze_orders as (
    select * from {{ ref('bronze_orders') }}
),

silver_customers as (
    select * from {{ ref('silver_customers') }}
),

cleaned as (
    select
        -- Surrogate keys
        {{ dbt_utils.generate_surrogate_key(['o.order_id']) }} as order_key,
        c.customer_key,
        
        -- Natural keys
        o.order_id,
        o.customer_id,
        
        -- Date fields
        o.order_date,
        
        -- Standardized status
        case 
            when lower(o.order_status) in ('completed', 'complete', 'success', 'delivered') 
                then 'completed'
            when lower(o.order_status) in ('pending', 'processing', 'in_progress') 
                then 'pending'
            when lower(o.order_status) in ('cancelled', 'canceled', 'failed', 'returned') 
                then 'cancelled'
            else 'unknown'
        end as order_status,
        
        -- Business logic flags
        case 
            when lower(o.order_status) in ('completed', 'complete', 'success', 'delivered') 
                then true 
            else false 
        end as is_completed,
        
        case 
            when lower(o.order_status) in ('cancelled', 'canceled', 'failed', 'returned') 
                then true 
            else false 
        end as is_cancelled,
        
        case 
            when lower(o.order_status) in ('pending', 'processing', 'in_progress') 
                then true 
            else false 
        end as is_pending,
        
        -- Amount
        o.order_amount,
        
        -- Date components
        extract(year from o.order_date) as order_year,
        extract(month from o.order_date) as order_month,
        extract(day from o.order_date) as order_day,
        extract(quarter from o.order_date) as order_quarter,
        dayname(o.order_date) as order_day_name,
        to_char(o.order_date, 'YYYY-MM') as order_year_month,
        to_char(o.order_date, 'YYYY-Q') as order_year_quarter,
        
        -- Audit columns
        o._loaded_at,
        current_timestamp() as _updated_at,
        
        -- Data quality flags
        case 
            when o.order_amount <= 0 then 1
            when o.order_date is null then 1
            when c.customer_key is null then 1
            else 0
        end as _has_data_quality_issues
        
    from bronze_orders o
    inner join silver_customers c 
        on o.customer_id = c.customer_id
    
    -- Filter out invalid records
    where o.order_amount > 0
        and o.order_date is not null
)

select * from cleaned