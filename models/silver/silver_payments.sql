{{config(materialized='table',database='SILVER')}}

/*
    Silver Layer - Payments
    
    Purpose: Validate and standardize payment transactions
    Source: Bronze payments
    Transformations:
        - Generate surrogate keys
        - Join to order fact
        - Standardize payment methods
        - Add business logic flags
        - Validate amounts
    
    Grain: One row per payment transaction
    Quality: Only payments linked to valid orders
*/

with bronze_payments as (
    select * from {{ ref('bronze_payments') }}
),

silver_orders as (
    select * from {{ ref('silver_orders') }}
),

cleaned as (
    select
        -- Surrogate keys
        {{ dbt_utils.generate_surrogate_key(['p.payment_id']) }} as payment_key,
        o.order_key,
        
        -- Natural keys
        p.payment_id,
        p.order_id,
        
        -- Standardized payment method
        case 
            when lower(p.payment_method) in ('credit_card', 'credit card', 'cc', 'visa', 'mastercard', 'amex') 
                then 'credit_card'
            when lower(p.payment_method) in ('bank_transfer', 'bank transfer', 'wire', 'ach', 'direct_debit') 
                then 'bank_transfer'
            when lower(p.payment_method) in ('paypal', 'pp') 
                then 'paypal'
            when lower(p.payment_method) in ('cash', 'cash on delivery', 'cod') 
                then 'cash'
            when lower(p.payment_method) in ('apple_pay', 'google_pay', 'digital_wallet') 
                then 'digital_wallet'
            else 'other'
        end as payment_method,
        
        -- Amount
        p.payment_amount,
        
        -- Timestamp
        p.payment_date,
        
        -- Business logic flags
        case 
            when p.payment_amount > 0 then true 
            else false 
        end as is_successful_payment,
        
        case 
            when p.payment_amount <= 0 then true 
            else false 
end as is_failed_payment,
        
        -- Payment categorization
        case 
            when p.payment_amount < 50 then 'small'
            when p.payment_amount < 200 then 'medium'
            when p.payment_amount < 500 then 'large'
            else 'very_large'
        end as payment_size_category,
        
        -- Audit columns
        p._loaded_at,
        current_timestamp() as _updated_at,
        
        -- Data quality flags
        case 
            when p.payment_amount <= 0 then 1
            when p.payment_date is null then 1
            when o.order_key is null then 1
            else 0
        end as _has_data_quality_issues
        
    from bronze_payments p
    inner join silver_orders o 
        on p.order_id = o.order_id
)

select * from cleaned