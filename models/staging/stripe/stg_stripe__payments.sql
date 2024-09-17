select 
    id as payment_id,
    orderid as order_id,
    amount/100 as amount,
    status
    from {{ source('dbt_testing', 'stripe_payment') }}