with

payments as (

    select * from {{ ref('stg_stripe__payments_ref_test') }}

),

transformed as (

        select 
            order_id, 
            max(payment_creation) as payment_finalized_date, 
            sum(payment_amount) / 100.0 as total_amount_paid
        from payments
        where payment_status <> 'fail'
        group by 1
)

select * from transformed