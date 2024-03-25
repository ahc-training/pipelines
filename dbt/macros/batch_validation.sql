{% macro batch_validation(fields) %}
    {% for field in fields %}
        {{ field }} > (select max(coalesce({{ field }}, '1900-01-01')) from {{ this }})
        {{ " OR " if not loop.last }}
    {% endfor %}
{% endmacro %}