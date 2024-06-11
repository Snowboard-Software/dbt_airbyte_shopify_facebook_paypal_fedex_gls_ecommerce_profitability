/*
List of products by product id, title, handle, and list of variants in the options array
*/

with 

source as (

    select * from {{ source('raw_airbyte_data', 'shopify_products') }}

),

renamed as (

    select
        _airbyte_raw_id,
        _airbyte_extracted_at,
        id,
        tags,
        title,
        handle,
        status,
        options,
        created_at,
        deleted_at,
        updated_at,
        product_type,
        published_at,
        published_scope,
        template_suffix

    from source

)

select * from renamed
