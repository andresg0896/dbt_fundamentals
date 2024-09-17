-- Refunds have a negative amount, so the total amount should always be >= 0.
-- Therefore return records where this isn't true to make the test fail.
with payment AS (
    select * from {{ ref('stg_stripe__payments') }}
)

,
select
    order_id,
    sum(amount) as total_amount
from payment
group by order_id
having total_amount < 0