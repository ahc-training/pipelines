with current as (
  select
    first_name,
    last_name,
    max(last_update) as last_update
  from {{ source("dvdrental", "customer") }}
  group by first_name, last_name
)

select
  source.*
from {{ source("dvdrental", "customer") }} source
inner join current
  on source.first_name = current.first_name
    and source.last_name = current.last_name
    and source.last_update = coalesce(current.last_update, '1900-01-01')