{% test rowcount_gt_0(model) %}

select 1 as failure
where (select count(*) from {{ model }}) = 0

{% endtest %}