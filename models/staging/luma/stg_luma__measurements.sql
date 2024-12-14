with

source as (

    select * from {{ source('luma', 'measurements') }}

),

transformed as (

    select 

        date,
        cast(timestamp as time) as measurement_time,
        client_id,
        group_id,
        main_board.id as main_board_id,
        sub_group.id as sub_group_id,
        variable.id as variable_id,
        variable.value as variable_value

    from source
    left join unnest(main_board) as main_board
    left join unnest(main_board.sub_group) as sub_group
    left join unnest(sub_group.sensor_board) as sensor_board
    left join unnest(sensor_board.port) as port
    left join unnest(port.variable) as variable    
    
    where
    date > "2024-11-14"
    
)

select * from transformed