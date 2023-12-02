with
  input(input) as (
    from read_csv_auto('input.csv')
  )

  , first_and_last_digits as (
    select
      input
      , regexp_extract(input, '\d') as first_digit_str
      , case first_digit_str
          when '' then null
          else first_digit_str
        end as first_digit
      , instr(input, first_digit) as first_digit_position
      , regexp_extract(reverse(input), '\d') as last_digit_str
      , case last_digit_str
          when '' then null
          else last_digit_str
        end as last_digit
      , instr(reverse(input), last_digit) as last_digit_position
      , [first_digit, last_digit]::int[2] as digits
    from input
  )

  , part_1 as (
    select
      1 as part
      , sum(
          (first_digit_str || last_digit_str)::int
      ) as answer
    from first_and_last_digits
  )

  , word_to_digit_mapping (word, digit) as (
    values
      (  ('one'),   (1))
      , (('two'),   (2))
      , (('three'), (3))
      , (('four'),  (4))
      , (('five'),  (5))
      , (('six'),   (6))
      , (('seven'), (7))
      , (('eight'), (8))
      , (('nine'),  (9))
  )

  , word_positions as (
    select
      *
      , regexp_extract(input, word) as first_word
      , case
          when first_word != '' then instr(input, first_word)
        end as first_word_position
      , regexp_extract(reverse(input), reverse(word)) as last_word
      , case
          when last_word != '' then instr(reverse(input), last_word)
        end as last_word_position
    from first_and_last_digits
    cross join word_to_digit_mapping
  )

  , first_and_last_word_positions as (
    select
      input
      , arg_min(digit, first_word_position) as first_word
      , min(first_word_position) as first_word_position
      , arg_min(digit, last_word_position) as last_word
      , min(last_word_position) as last_word_position
    from word_positions
    group by all
  )

 , first_and_last_word_or_digit as (
    select
      (
        case
          when first_word_position < first_digit_position or first_digit is null
          then first_word
          else first_digit
        end
        ||
        case
          when last_word_position < last_digit_position or last_digit is null
          then last_word
          else last_digit
        end
      )::int as digits
    from first_and_last_word_positions
    join first_and_last_digits
    using (input)
 )

 , part_2 as (
    select
      2 as part
      , sum(digits) as answer
    from first_and_last_word_or_digit
 )

select *
from part_1

union all

select *
from part_2
