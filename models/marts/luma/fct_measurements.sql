with measurements as(
    select * from {{ ref("int_measurements") }}
),

stdev_grouped as(

    select 
        date,
        start_time,
        end_time,
        client_id,
        group_id,
        main_board_id,
        sub_group_id,
        variable_id,
        round(avg(variable_value),4) as mean,
        round(coalesce(stddev(variable_value),0),4) as st_dev
     
    from measurements
    group by 1,2,3,4,5,6,7,8
)

select * from stdev_grouped