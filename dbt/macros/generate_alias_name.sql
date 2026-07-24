{% macro generate_alias_name(custom_alias_name=none, node-none) -%}
    {%- if target.name in ('dev') -%}
        {{custom_alias_name if custom_alias_name is not none else node.name }}
    {% elif custom_alias_name is none -%}
        {{ node.name }}
    {%- else -%}
        {{ custom_alias_name | trim }}
    {%- endif -%}
{%- endmacro %}