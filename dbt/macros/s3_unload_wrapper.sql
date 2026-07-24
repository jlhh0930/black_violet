{% macro normalize_model_name(name) -%}
    {#
        - remove quotes
        - convert to lowercase
        - remove __dbt_tmp suffixes where present
        - remove hyper_ prefix where present
    #}
    {% set re = modules.re %}
    {% set n = name | replace('\"', '') | lower | trim %}
    {% set n = re.sub('__dbt_tmp.*', '', n, re.IGNORECASE) | trim %}
    {% set n = re.sub('^hyper_', '', n, re.IGNORECASE) %}
    {{ return(n) }}
{%- endmacro %}

{% macro generate_s3_unload_name(bucket=None, prefix=None, model_name=None, relation_name=None) -%}
    {#
        Args:
            bucket: S3 bucket
            prefix: S3 prefix
            model_name: logical model or table name
            relation_name: physical relation name

        Returns:
            S3 path string: s3://{bucket}/{prefix}/{model}/
    #}
    {# Default to 'this' if model_name not provided #}
    {% if not model_name and this is defined %}
        {% set model_name = this.identifier %}
    {% endif %}

    {# Build path from bucket/prefix #}
    {% set b = bucket or var('staging_bucket', none) %}
    {% set p = prefix if prefix is not none else var('staging_prefix', '') %}
    {% if not b %}
        {% do exceptions.raise_compiler_error('s3_unload_path or staging_bucket must be provided via vars') %}
    {% endif %}

    {% set p = (p | string).strip('/') %}
    {% if p %}
        {# Use explicit prefix #}
        {% set path = 's3://' ~ b ~ '/' ~ p %}
    {% else %}
        {# Default path structure with no prefix: s3://bucket/model/ #}
        {% set cleaned_name = relation_name | string | replace('\"', '') | lower | trim if relation_name else '' %}
        {% set cleaned_name = re.sub('__dbt_tmp.*', '', re.IGNORECASE) | trim if cleaned_name else '' %}
        {% set generated_clean_prefix = cleaned_name or 'unknown_model' %}
        {% set path = 's3://' ~ b ~ '/' ~ generated_clean_prefix ~ '/'}
    {% endif %}

    {% do log('S3 Location [' ~ path ~ ']', True) %}
{%- endmacro %}

{% macro unload_relation_to_s3(table_name, format='csv', logical_name=None) -%}
    {% set model = logical_name or table_name.identifier %}

    {% set model = normalize_model_name(model) %}

    {% set prefix = var('s3_prefix', None) %}

    {% set bucket = var('s3_bucket') %}

    {% set s3_path = generate_s3_unload_name(bucket=bucket, prefix=prefix, model_name=model, relation_name=table_name.identifier) %}

    {% set iam_role = var('iam_role', none) %}
    {% if iam_role is none %}
        {% do exceptions.raise_compiler_error('iam_role must be supplied via vars') %}
    {% endif %}

    {% set max_file_size = var('max_file_size', '1 GB') %}

    {% set query %}
        UNLOAD (
            $$
                SELECT * FROM {{ table_name }}
            $$)
        TO '{{ s3_path }}/{{ var("report_name") }}_{{ var("formatted_job_timestamp_start") }}_{{ var("report_start_date" ) }}_{{ var("report_end_date") }}_'
        IAM_ROLE '{{ iam_role }}'
        DELIMITER '|'
        HEADER
        PARALLEL FALSE
        ALLOWOVERWRITE
        MAXFILESIZE {{ max_file_size }}
        GZIP
    {% endset %}

    {% do log('Unloading data from ' ~ table_name.identifier ~ ' to ' s3_path, info=True) %}
    {{ return(query) }}
{%- endmacro %}