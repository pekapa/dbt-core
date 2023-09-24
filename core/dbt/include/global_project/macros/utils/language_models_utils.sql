{% macro resolve_model_name(input_model_name) %}
    {{ return(adapter.dispatch('resolve_model_name', 'dbt')(input_model_name)) }}
{% endmacro %}

{%- macro default__resolve_model_name(input_model_name) -%}
    {{  input_model_name | string | replace('"', '\"') }}
{%- endmacro -%}

{% macro build_ref_dict(model) %}
    {%- set ref_dict = {} -%}
    {%- for _ref in model.refs -%}
        {% set _ref_args = [_ref.get('package'), _ref['name']] if _ref.get('package') else [_ref['name'],] %}
        {%- set resolved = ref(*_ref_args, v=_ref.get('version')) -%}
        {%- if _ref.get('version') -%}
            {% do _ref_args.extend(["v" ~ _ref['version']]) %}
        {%- endif -%}
       {%- do ref_dict.update({_ref_args | join('.'): resolve_model_name(resolved)}) -%}
    {%- endfor -%}
    {{ return(ref_dict) }}
{% endmacro %}

{% macro build_source_dict(model) %}
    {%- set source_dict = {} -%}
    {%- for _source in model.sources -%}
        {%- set resolved = source(*_source) -%}
        {%- do source_dict.update({_source | join('.'): resolve_model_name(resolved)}) -%}
    {%- endfor -%}
    {{ return(source_dict) }}
{% endmacro %}

{% macro build_config_dict(model) %}
    {%- set config_dict = {} -%}
    {% set config_dbt_used = zip(model.config.config_keys_used, model.config.config_keys_defaults) | list %}
    {%- for key, default in config_dbt_used -%}
        {# weird type testing with enum, would be much easier to write this logic in Python! #}
        {%- if key == "language" -%}
          {%- set value = "scala" -%}
        {%- endif -%}
        {%- set value = model.config.get(key, default) -%}
        {%- do config_dict.update({key: value}) -%}
    {%- endfor -%}
    {{ return(config_dict) }}
{% endmacro %}