{% macro build_py_ref_function(model) %}

def ref(*args, **kwargs):
    refs = {{ build_ref_dict(model) | tojson }}
    key = '.'.join(args)
    version = kwargs.get("v") or kwargs.get("version")
    if version:
        key += f".v{version}"
    dbt_load_df_function = kwargs.get("dbt_load_df_function")
    return dbt_load_df_function(refs[key])

{% endmacro %}

{% macro build_py_source_function(model) %}

def source(*args, dbt_load_df_function):
    sources = {{ build_source_dict(model) | tojson }}
    key = '.'.join(args)
    return dbt_load_df_function(sources[key])
{% endmacro %}

{% macro build_py_config_dict(model) %}
config_dict = {{ build_config_dict(model) }}
{% endmacro %}

{% macro py_script_postfix(model) %}
# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

{{ build_py_ref_function(model ) }}
{{ build_py_source_function(model ) }}
{{ build_py_config_dict(model) }}

class config:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def get(key, default=None):
        return config_dict.get(key, default)

class this:
    """dbt.this() or dbt.this.identifier"""
    database = "{{ this.database }}"
    schema = "{{ this.schema }}"
    identifier = "{{ this.identifier }}"
    {% set this_relation_name = resolve_model_name(this) %}
    def __repr__(self):
        return '{{ this_relation_name  }}'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = {{ is_incremental() }}

# COMMAND ----------
{{py_script_comment()}}
{% endmacro %}

{#-- entry point for add instuctions for running compiled_code --#}
{%macro py_script_comment()%}
{%endmacro%}
