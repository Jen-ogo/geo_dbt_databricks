{% test not_empty(model) %}
select 1
where (select count(*) from {{ model }}) = 0
{% endtest %}