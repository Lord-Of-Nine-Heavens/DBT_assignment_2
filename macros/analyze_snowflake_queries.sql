{% macro analyze_snowflake_queries(days_back=7, limit=20) %}

  {% if target.name == 'dev' %}

    {% set query %}
        select 
            query_id, 
            warehouse_name,
            user_name,
            execution_status,
            start_time,
            total_elapsed_time / 1000 as total_execution_time_seconds,
            bytes_scanned / power(1024, 3) as bytes_scanned_gb,
            partitions_scanned,
            partitions_total,
            case 
                when partitions_total > 0 then round((partitions_scanned::float / partitions_total) * 100, 2)
                else null 
            end as partition_scan_percentage,
            compilation_time / 1000 as compilation_time_seconds,
            execution_time / 1000 as execution_time_seconds,
            (queued_provisioning_time + queued_repair_time + queued_overload_time) / 1000 as total_queue_time_seconds,
            bytes_spilled_to_local_storage / power(1024, 3) as spillage_to_local_gb,
            bytes_spilled_to_remote_storage / power(1024, 3) as spillage_to_remote_gb,
            credits_used_cloud_services as credits_used,
            database_name,
            schema_name,
            query_text
        from snowflake.account_usage.query_history
        where start_time >= dateadd(day, -{{ days_back }}, current_timestamp())
        and database_name = '{{ target.database }}'
        order by total_execution_time_seconds desc
        limit {{ limit }}
    {% endset %}

    {% if execute %}
        {% set results = run_query(query) %}
        
        {% if results|length > 0 %}
            {% for row in results %}
                {% set credits_used_formatted = "%.8f" % row['credits_used'] %}
                {{ log("✅ query execution log for " ~ row['query_id'], info=true) }}
                {{ log("status: " ~ row['execution_status'], info=true) }}
                {{ log("credits used: " ~ credits_used_formatted ~ " credits", info=true) }}
                {{ log("user: " ~ row['user_name'], info=true) }}
                {{ log("warehouse: " ~ row['warehouse_name'], info=true) }}
                {{ log("database: " ~ row['database_name'], info=true) }}
                {{ log("schema: " ~ row['schema_name'], info=true) }}
                {{ log("start time: " ~ row['start_time'], info=true) }}
                {{ log("total execution time: " ~ row['total_execution_time_seconds'] ~ " sec", info=true) }}
                {{ log("bytes scanned: " ~ row['bytes_scanned_gb'] ~ " gb", info=true) }}
                {{ log("partitions scanned: " ~ row['partitions_scanned'], info=true) }}
                {{ log("partition scan %: " ~ row['partition_scan_percentage'] ~ " %", info=true) }}
                {{ log("compilation time: " ~ row['compilation_time_seconds'] ~ " sec", info=true) }}
                {{ log("execution time: " ~ row['execution_time_seconds'] ~ " sec", info=true) }}
                {{ log("total queue time: " ~ row['total_queue_time_seconds'] ~ " sec", info=true) }}
                {{ log("spillage to local: " ~ row['spillage_to_local_gb'] ~ " gb", info=true) }}
                {{ log("spillage to remote: " ~ row['spillage_to_remote_gb'] ~ " gb", info=true) }}
                {{ log("query text: " ~ row['query_text'], info=true) }}
            {% endfor %}
        {% else %}
            {{ log("⚠ no queries found for analysis.", info=true) }}
        {% endif %}

        {{ return(results) }}

    {% endif %}

  {% else %}
    {{ log("🚫 skipping execution: this macro runs only in 'dev' environment.", info=true) }}
  {% endif %}

{% endmacro %}
