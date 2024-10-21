with

source as (

    select * from {{ source('stripe', 'payment') }}

),

transformed as (

    select 

        id as payment_id,
        orderid as order_id,
        round(amount/100, 2) as payment_amount,
        status as payment_status

    from source

)

select * from transformed