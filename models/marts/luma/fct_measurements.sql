{{
  config(
    materialized = "table",
    sort = ['date','start_time', 'end_time', 'client_id', 'group_id', 
    'main_board_id','sub_group_id', 'variable_id']

  )
}}

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
        count(variable_id) as variable_count,
        round(avg(variable_value),4) as variable_mean,
        round(coalesce(stddev(variable_value),1000000),4) as variable_stddev,
        min(variable_value) as variable_min_value,
        max(variable_value) as variable_max_value,
        0 as is_outlier
    from measurements
    where
        variable_value between variable_mean - 1.5*variable_std_dev and variable_mean + 1.5*variable_std_dev
    group by 1,2,3,4,5,6,7,8
),

outliers as(

    select 
        date,
        start_time,
        end_time,
        client_id,
        group_id,
        main_board_id,
        sub_group_id,
        variable_id,
        1 as variable_count,
        variable_value as variable_mean,
        1000000 as variable_stddev,
        variable_value as variable_min_value,
        variable_value as variable_max_value,
        1 as is_outlier
    from measurements
    where
        variable_value not between variable_mean - 1.5*variable_std_dev and variable_mean + 1.5*variable_std_dev

),

final as(

    select 
        *
    from stdev_grouped
    union all 
    select
        *
    from outliers

)

select * from final