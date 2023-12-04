with
  input as (
    select
      row_number() over () as row
      , #1 as input
      , regexp_extract_all(#1, '(\d+)') as part_numbers
    from read_csv_auto('input.csv')
  )

  , rows as (
    select
      row
      , unnest(
        split(input, '')
      ) as input
    from input
  )

  , grid as (
    select
      row
      , row_number() over (partition by row) as col
      , input
    from rows
    order by
      row
      , col
  )

  , part_number_groups as (
    select
      row
      , col
      , input
      , regexp_matches(input, '\d') as is_part_number
      , col - row_number() over (
          partition by row
          , not is_part_number
          order by col
        ) as part_number_group
    from grid
  )

  , part_numbers as (
    select
      row
      , part_number_group
      , [min(col), max(col)] as anchors
      , string_agg(input, '')::int as part_number
    from part_number_groups
    where is_part_number
    group by all
  )

  , symbols as (
    select
      row
      , col
      , input as symbol
    from grid
    where regexp_matches(input, '([^\.|^\d])')
  )

  , joined as (
    select
      grid.row
      , grid.col
      , symbols.symbol
      , part_numbers.anchors
      , part_numbers.part_number
    from grid
    left join symbols on
      grid.row = symbols.row
      and grid.col = symbols.col
    left join part_numbers on (
      grid.row = part_numbers.row -- check current row
      and (
        list_contains(part_numbers.anchors, symbols.col - 1) -- ← left
        or list_contains(part_numbers.anchors, symbols.col + 1) -- → right
      )
    )
    or (
      grid.row - 1 = part_numbers.row -- check row above
      and (
        list_contains(part_numbers.anchors, symbols.col) -- ↑ up
        or list_contains(part_numbers.anchors, symbols.col - 1) -- ↖ upper left
        or list_contains(part_numbers.anchors, symbols.col + 1) -- ↗ upper right
      )
    )
    or (
      grid.row + 1 = part_numbers.row -- check row below
      and (
        list_contains(part_numbers.anchors, symbols.col) -- ↓ down
        or list_contains(part_numbers.anchors, symbols.col - 1) -- ↙ down left
        or list_contains(part_numbers.anchors, symbols.col + 1) -- ↘ down right
      )
    )
    order by
      grid.row
      , grid.col
  )

  , gear_ratios as (
    select
      row
      , col
      , product(part_number)::int as gear_ratio
    from joined
    where symbol = '*'
    group by all
    having count(*) = 2
  )

select
  1 as part
  , sum(part_number) as answer
from joined

union all

select
  2 as part
  , sum(gear_ratio) as answer
from gear_ratios
