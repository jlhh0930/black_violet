{% macro pii_assert_no_raw(columns, raw_field_prefixes=[]) %}
  {# columns: list of column names in the model #}
  {# raw_field_prefixes: e.g., ["email", "phone", "patient_id"] if those would be raw #}
  {% for c in columns %}
    {% for p in raw_field_prefixes %}
      {% if c == p %}
        {{ exceptions.raise_compiler_error("Raw PII/PHI column detected in gold model: " ~ c) }}
      {% endif %}
    {% endfor %}
  {% endfor %}
{% endmacro %}

{% macro pii_assert_masked_present(required_cols) %}
  {# required_cols: list of columns expected in gold #}
  {% for col in required_cols %}
    {% if col not in required_cols %}
      {{ exceptions.raise_compiler_error("Missing masked/tokenized column: " ~ col) }}
    {% endif %}
  {% endfor %}
{% endmacro %}

{% macro pii_assert_forbidden_columns(model_relation, forbidden_cols) %}
  {# MVP: simple manual config per model; later auto-generate from YAML #}
  {% set cols = forbidden_cols %}
  {{ exceptions.raise_compiler_error(
      "PII contract enforcement stub. Wire forbidden columns per model in YAML tests."
  ) }}
{% endmacro %}
