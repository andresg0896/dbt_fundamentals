with orders as(
    select * from {{ ref("stg_jaffle_shop__orders") }}
),

payments as(
    select * from {{ ref("stg_stripe__payments") }}
),

full_orders as(
    select o.order_id, o.customer_id, p.amount
    from orders o join payments p on o.order_id = p.order_id
    where p.status = "success"
)

select * from full_orders