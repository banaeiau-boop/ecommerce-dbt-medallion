{{
    config(
        materialized='view',
        tags=['bronze', 'daily', 'orders']
    )
}}

/*
    Bronze Layer - Orders
    
    Purpose: Ingest raw order data with minimal transformation
    Source: RAW_DATA.ECOMMERCE.raw_orders
    Transformations:
        - Column renaming for consistency
        - Preserve all raw data
    
    Grain: One row per order
*/

with source as (
    select * from {{ source('raw_ecommerce', 'raw_orders') }}
),

renamed as (
    select
        -- Primary key
        id as order_id,
        
        -- Foreign keys
        user_id as customer_id,
        
        -- Order attributes
        order_date,
        status as order_status,
        amount as order_amount,
        
        -- Audit columns
        _loaded_at,
        'bronze_orders' as _source_model
        
    from source
)

select * from renamed