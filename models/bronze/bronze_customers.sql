{{
    config(
        materialized='view',
        tags=['bronze', 'daily', 'customers']
    )
}}

/*
    Bronze Layer - Customers
    
    Purpose: Ingest raw customer data with minimal transformation
    Source: RAW_DATA.ECOMMERCE.raw_customers
    Transformations:
        - Column renaming for consistency
        - Add audit timestamp
    
    Grain: One row per customer
*/

with source as (
    select * from {{ source('raw_ecommerce', 'raw_customers') }}
),

renamed as (
    select
        -- Primary key
        id as customer_id,
        
        -- Customer attributes
        first_name,
        last_name,
        email,
        created_at,
        
        -- Audit columns
        current_timestamp() as _loaded_at,
        'bronze_customers' as _source_model
        
    from source
)

select * from renamed