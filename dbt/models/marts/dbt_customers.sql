with customers as (
    select
         first_name
        ,last_name
        ,email
        ,last_update
    from {{ ref("dvdrental_customer") }}
    where active = 0
)

select
     {{ dbt_utils.generate_surrogate_key(["customers.first_name", "customers.last_name"]) }} as customer_sk
    ,customers.first_name
    ,customers.last_name
    ,customers.last_update
from customers
{% if is_incremental() %}
where 
    {{ batch_validation(["last_update"]) }}
{% endif %}