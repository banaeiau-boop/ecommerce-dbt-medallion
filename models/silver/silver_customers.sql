{{config(materialized='table',database='SILVER')}}

/*
    Silver Layer - Customers
    
    Purpose: Clean, validate, and standardize customer data
    Source: Bronze customers
    Transformations:
        - Generate surrogate keys
        - Standardize names (proper case)
        - Standardize email (lowercase)
        - Create full_name field
        - Filter out invalid records
    
    Grain: One row per customer
    Quality: Only valid customers with required fields
*/

with bronze_customers as (
    select * from {{ ref('bronze_customers') }}
),

cleaned as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
        
        -- Natural key
        customer_id,
        
        -- Cleaned name fields
        initcap(trim(first_name)) as first_name,
        initcap(trim(last_name)) as last_name,
        concat(
            initcap(trim(first_name)), 
            ' ', 
            initcap(trim(last_name))
        ) as full_name,
        
        -- Standardized email (lowercase, trimmed)
        lower(trim(email)) as email,
        
        -- Timestamps
        created_at,
        _loaded_at,
        current_timestamp() as _updated_at,
        
        -- Data quality flags
        case 
            when first_name is null then 1
            when last_name is null then 1
            when email is null then 1
            when email not like '%@%.%' then 1
            else 0
        end as _has_data_quality_issues
        
    from bronze_customers
    
    -- Filter out invalid records
    where email is not null
        and first_name is not null
        and last_name is not null
        and email like '%@%.%'  
)

select * from cleaned