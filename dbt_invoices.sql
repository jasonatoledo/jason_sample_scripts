-- AWS Redshift Syntax
{{ 
    config(
        materialized='table'
        , tags=["payment_platform", "invoices","fivetran"]
        , schema='dbt_invoices'
        , pre_hook="begin transaction;
            grant select on all tables in schema fivetran_invoices to group dbt_users;"
        , post_hook="commit; 
                     grant select on {{ this }} to looker;
                     grant select on {{ this }} to group dbt_users;"
    ) 
}}

with source_data as(

    select  {{ dbt_utils.star(from=source('fivetran_invoices','invoice'), except=["_fivetran_synced","_fivetran_deleted"],
                quote_identifiers=False) }}
            , {{ dbt_utils.star(from=source('fivetran_invoices','invoice_discount'), except=["_fivetran_synced","_fivetran_deleted","_fivetran_id"],
                quote_identifiers=False) }}

    from {{ source ('fivetran_invoices','invoice') }} inv
    left join {{ source ('fivetran_invoices','invoice_discount') }} invd on (
        inv.id = invd.invoice_id and 
        entity_id is not null and
        entity_type = 'document_level_coupon'
    )
    
)

select
    
    id
    , {{ convert_unix_epoch_time('date') }}::timestamp as created_date
    , {{ convert_unix_epoch_time('generated_at') }}::timestamp as generated_at_date
    , {{ convert_unix_epoch_time('due_date') }}::timestamp as due_date
    , {{ convert_unix_epoch_time('paid_at') }}::timestamp as paid_at_date
    , {{ convert_unix_epoch_time('updated_at') }}::timestamp as updated_date
    , customer_id
    , status
    , amount_due*1.0/100 as amount_due
    , amount_to_collect*1.0/100 as amount_to_collect    
    , amount_adjusted*1.0/100 as amount_adjusted
    , sub_total *1.0/100 as subtotal   
    , amount*1.0/100 as discount
    , credits_applied*1.0/100 as credits_applied    
    , total *1.0/100 as total
    , amount_paid*1.0/100 as amount_paid    
    , discount_type    
    , new_sales_amount *1.0/100 as new_sales_amount
    , first_invoice      
    , recurring    
    , channel
    , currency_code
    , deleted
    , exchange_rate
    , has_advance_charges
    , is_gifted
    , net_term_days
    , subscription_id
    , term_finalized
    , write_off_amount
    , {{ convert_unix_epoch_time('resource_version') }}::timestamp as resource_version

from source_data
