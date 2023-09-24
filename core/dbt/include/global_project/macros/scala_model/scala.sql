{% macro scala_script_prefix(model) %}
// this part is dbt logic for get ref work, do not modify
// you will need to copy the next section to run the code

import org.apache.spark.sql.{DataFrame, SparkSession}
import sqlContext.implicits.StringToColumn
import org.json4s.jackson.JsonMethods.{parse => parse_json}


case class configObj(options: Map[String, Any])
object configObj {
    val cfg = parse_json("""{{ build_config_dict(model) | tojson }}""").values.asInstanceOf[Map[String, String]]
    def get = cfg.getOrElse _
    def get(key: String): String = {
        val v = cfg.get(key)
        if (v.isDefined) v.get else None.toString
    }
}


class dbtObj(load_df_function: String => DataFrame) {
    val is_incremental = {{ is_incremental() | lower }}
    val database = "{{ this.database }}"
    val schema = "{{ this.schema }}"
    val identifier = "{{ this.identifier | lower }}"
    {% set this_relation_name = resolve_model_name(this) %}
    override def toString = "{{ this_relation_name }}"
    val source_map =  parse_json("""{{ build_source_dict(model) | tojson }}""").values.asInstanceOf[Map[String, String]]
    val ref_map = parse_json("""{{ build_ref_dict(model) | tojson }}""").values.asInstanceOf[Map[String, String]]
    val config = configObj

    def source(args: String*): DataFrame = {
        val key = args.mkString(".")
        load_df_function(source_map(key))
    }

    def ref(args: String*): DataFrame = {
        val key = args.mkString(".")
        load_df_function(ref_map(key))
    }
}

// COMMAND ----------
{{scala_script_comment()}}
// COMMAND ----------
// This part is user provided model code
{% endmacro %}

{#-- entry point for add instuctions for running compiled_code --#}
{%macro scala_script_comment()%}
{%endmacro%}