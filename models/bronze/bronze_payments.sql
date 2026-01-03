
{{
    config(
        materialized='view',
        tags=['bronze', 'daily', 'payments']
    )
}}

/*
    Bronze Layer - Payments
    
    Purpose: Ingest raw payment data with minimal transformation
    Source: RAW_DATA.ECOMMERCE.raw_payments
    Transformations:
        - Column renaming for consistency
        - Add audit timestamp
    
    Grain: One row per payment transaction
*/

with source as (
    select * from {{ source('raw_ecommerce', 'raw_payments') }}
),

renamed as (
    select
        -- Primary key
        id as payment_id,
        
        -- Foreign keys
        order_id,
        
        -- Payment attributes
        payment_method,
        amount as payment_amount,
        payment_date,
        
        -- Audit columns
        current_timestamp() as _loaded_at,
        'bronze_payments' as _source_model
        
    from source
)

select * from renamed