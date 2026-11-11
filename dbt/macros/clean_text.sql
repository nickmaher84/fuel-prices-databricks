{% macro clean_text(column) -%}
nullif(nullif(trim({{ column }}), ''), 'NULL')
{%- endmacro %}
