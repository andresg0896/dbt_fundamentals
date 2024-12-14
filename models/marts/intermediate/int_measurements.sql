with

measurements as (

    select * from {{ ref('stg_luma__measurements') }}

),

transformed as (

        select 
            date,
            measurement_time,
            CASE 
              WHEN measurement_time BETWEEN '00:00:00' AND '03:59:59' then '00:00:00'
              WHEN measurement_time BETWEEN '04:00:00' AND '07:59:59' then '04:00:00'
              WHEN measurement_time BETWEEN '08:00:00' AND '11:59:59' then '08:00:00'
              WHEN measurement_time BETWEEN '12:00:00' AND '15:59:59' then '12:00:00'
              WHEN measurement_time BETWEEN '16:00:00' AND '19:59:59' then '16:00:00'
              WHEN measurement_time BETWEEN '20:00:00' AND '23:59:59' then '20:00:00'
            END AS start_time,
            CASE 
              WHEN measurement_time BETWEEN '00:00:00' AND '03:59:59' then '04:00:00'
              WHEN measurement_time BETWEEN '04:00:00' AND '07:59:59' then '08:00:00'
              WHEN measurement_time BETWEEN '08:00:00' AND '11:59:59' then '12:00:00'
              WHEN measurement_time BETWEEN '12:00:00' AND '15:59:59' then '16:00:00'
              WHEN measurement_time BETWEEN '16:00:00' AND '19:59:59' then '20:00:00'
              WHEN measurement_time BETWEEN '20:00:00' AND '23:59:59' then '24:00:00'
            END AS end_time,
            client_id,
            group_id,
            main_board_id,
            sub_group_id,
            variable_id,
            variable_value
        from measurements
)

select * from transformed