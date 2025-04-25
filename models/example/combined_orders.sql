with customers as (

    select *
    from {{ source('raw', 'raw_customers') }}

),

orders as (

    select *
    from {{ source('raw', 'raw_orders') }}

),

payments as (

    select *
    from {{ source('raw', 'raw_payments') }}

),

-- Join orders to customers
orders_with_customers as (

    select
        o.id as order_id,
        o.user_id,
        o.order_date,
        o.status,
        c.first_name,
        c.last_name
    from orders o
    left join customers c on o.user_id = c.id

),

-- Join payments
final as (

    select
        oc.*,
        p.payment_method,
        p.amount
    from orders_with_customers oc
    left join payments p on oc.order_id = p.order_id

)

select * from final
