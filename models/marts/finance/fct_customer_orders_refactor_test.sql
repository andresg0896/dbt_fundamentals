with

customers as (

    select * from {{ ref('stg_jaffle_shop__customers_ref_test') }}

),

orders as (

    select * from {{ ref('stg_jaffle_shop__orders_ref_test') }}
),

payments as (

    select * from {{ ref('int_orders') }}

),

paid_orders as (

    select 
        orders.order_id,
        orders.customer_id,
        orders.order_placed_at,
        orders.order_status,
        p.total_amount_paid,
        p.payment_finalized_date,
        customers.customer_first_name,
        customers.customer_last_name
    from orders
    left join payments on orders.order_id = payments.order_id
    left join customers on orders.customer_id = customers.customer_id

),

unknown_paid as (

    select
        p.order_id,
        sum(t2.total_amount_paid) as clv_bad
    from paid_orders p
    left join paid_orders t2 on p.customer_id = t2.customer_id and p.order_id >= t2.order_id
    group by 1
    order by p.order_id

),

customer_orders as (

    select customers.customer_id,
        min(orders.order_placed_at) as first_order_date,
        max(orders.order_placed_at) as most_recent_order_date,
        count(orders.order_id) as number_of_orders
    from customers  
    left join orders
    on orders.customer_id = customers.customer_id 
    group by 1

),

final as(

    select
        p.*,
        row_number() over (order by p.order_id) as transaction_seq,
        row_number() over (partition by customer_id order by p.order_id) as customer_sales_seq,
        case when c.first_order_date = p.order_placed_at
        then 'new'
        else 'return' end as nvsr,
        x.clv_bad as customer_lifetime_value,
        c.first_order_date as fdos
    from paid_orders p
    left join customer_orders as c using (customer_id)
    left outer join unknown_paid on unknown_paid.order_id = p.order_id
    order by order_id
)

select * from final