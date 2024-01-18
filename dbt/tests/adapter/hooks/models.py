missing_column = """
select 1 as col
"""


hooks = """
select 1 as id
"""


hooks_configured = """
{{
    config({
        "pre_hook": "\
            insert into {{this.schema}}.on_model_hook (\
                test_state,\
                target_dbname,\
                target_host,\
                target_name,\
                target_schema,\
                target_type,\
                target_user,\
                target_pass,\
                target_threads,\
                run_started_at,\
                invocation_id,\
                thread_id
            ) VALUES (\
                'start',\
                '{{ target.dbname }}',\
                '{{ target.host }}',\
                '{{ target.name }}',\
                '{{ target.schema }}',\
                '{{ target.type }}',\
                '{{ target.user }}',\
                '{{ target.get(\\"pass\\", \\"\\") }}',\
                {{ target.threads }},\
                '{{ run_started_at }}',\
                '{{ invocation_id }}',\
                '{{ thread_id }}'\
        )",
        "post-hook": "\
            insert into {{this.schema}}.on_model_hook (\
                test_state,\
                target_dbname,\
                target_host,\
                target_name,\
                target_schema,\
                target_type,\
                target_user,\
                target_pass,\
                target_threads,\
                run_started_at,\
                invocation_id,\
                thread_id

            ) VALUES (\
                'end',\
                '{{ target.dbname }}',\
                '{{ target.host }}',\
                '{{ target.name }}',\
                '{{ target.schema }}',\
                '{{ target.type }}',\
                '{{ target.user }}',\
                '{{ target.get(\\"pass\\", \\"\\") }}',\
                {{ target.threads }},\
                '{{ run_started_at }}',\
                '{{ invocation_id }}',\
                '{{ thread_id }}'\
            )"
    })
}}

select 3 as id
"""


hooks_error = """
{{
    config({
        "pre_hook": "\
            insert into {{this.schema}}.on_model_hook (\
                test_state,\
                target_dbname,\
                target_host,\
                target_name,\
                target_schema,\
                target_type,\
                target_user,\
                target_pass,\
                target_threads,\
                run_started_at,\
                invocation_id,\
                thread_id
            ) VALUES (\
                'start',\
                '{{ target.dbname }}',\
                '{{ target.host }}',\
                '{{ target.name }}',\
                '{{ target.schema }}',\
                '{{ target.type }}',\
                '{{ target.user }}',\
                '{{ target.get(\\"pass\\", \\"\\") }}',\
                {{ target.threads }},\
                '{{ run_started_at }}',\
                '{{ invocation_id }}',\
                '{{ thread_id }}'
        )",
        "pre-hook": "\
            insert into {{this.schema}}.on_model_hook (\
                test_state,\
                target_dbname,\
                target_host,\
                target_name,\
                target_schema,\
                target_type,\
                target_user,\
                target_pass,\
                target_threads,\
                run_started_at,\
                invocation_id,\
                thread_id
            ) VALUES (\
                'start',\
                '{{ target.dbname }}',\
                '{{ target.host }}',\
                '{{ target.name }}',\
                '{{ target.schema }}',\
                '{{ target.type }}',\
                '{{ target.user }}',\
                '{{ target.get(\\"pass\\", \\"\\") }}',\
                {{ target.threads }},\
                '{{ run_started_at }}',\
                '{{ invocation_id }}',\
                '{{ thread_id }}'
        )",
        "post-hook": "\
            insert into {{this.schema}}.on_model_hook (\
                test_state,\
                target_dbname,\
                target_host,\
                target_name,\
                target_schema,\
                target_type,\
                target_user,\
                target_pass,\
                target_threads,\
                run_started_at,\
                invocation_id,\
                thread_id
            ) VALUES (\
                'end',\
                '{{ target.dbname }}',\
                '{{ target.host }}',\
                '{{ target.name }}',\
                '{{ target.schema }}',\
                '{{ target.type }}',\
                '{{ target.user }}',\
                '{{ target.get(\\"pass\\", \\"\\") }}',\
                {{ target.threads }},\
                '{{ run_started_at }}',\
                '{{ invocation_id }}',\
                '{{ thread_id }}'\
            )"
    })
}}

select 3 as id
"""


hooks_kwargs = """
{{
    config(
        pre_hook="\
            insert into {{this.schema}}.on_model_hook (\
                test_state,\
                target_dbname,\
                target_host,\
                target_name,\
                target_schema,\
                target_type,\
                target_user,\
                target_pass,\
                target_threads,\
                run_started_at,\
                invocation_id,\
                thread_id
            ) VALUES (\
                'start',\
                '{{ target.dbname }}',\
                '{{ target.host }}',\
                '{{ target.name }}',\
                '{{ target.schema }}',\
                '{{ target.type }}',\
                '{{ target.user }}',\
                '{{ target.get(\\"pass\\", \\"\\") }}',\
                {{ target.threads }},\
                '{{ run_started_at }}',\
                '{{ invocation_id }}',\
                '{{ thread_id }}'\
        )",
        post_hook="\
            insert into {{this.schema}}.on_model_hook (\
                test_state,\
                target_dbname,\
                target_host,\
                target_name,\
                target_schema,\
                target_type,\
                target_user,\
                target_pass,\
                target_threads,\
                run_started_at,\
                invocation_id,\
                thread_id\
            ) VALUES (\
                'end',\
                '{{ target.dbname }}',\
                '{{ target.host }}',\
                '{{ target.name }}',\
                '{{ target.schema }}',\
                '{{ target.type }}',\
                '{{ target.user }}',\
                '{{ target.get(\\"pass\\", \\"\\") }}',\
                {{ target.threads }},\
                '{{ run_started_at }}',\
                '{{ invocation_id }}',\
                '{{ thread_id }}'\
            )"
    )
}}

select 3 as id
"""


hooked = """
{{
    config({
        "pre_hook": "\
            insert into {{this.schema}}.on_model_hook select
                test_state,
                '{{ target.dbname }}' as target_dbname,\
                '{{ target.host }}' as target_host,\
                '{{ target.name }}' as target_name,\
                '{{ target.schema }}' as target_schema,\
                '{{ target.type }}' as target_type,\
                '{{ target.user }}' as target_user,\
                '{{ target.get(\\"pass\\", \\"\\") }}' as target_pass,\
                {{ target.threads }} as target_threads,\
                '{{ run_started_at }}' as run_started_at,\
                '{{ invocation_id }}' as invocation_id,\
                '{{ thread_id }}' as thread_id
                from {{ ref('pre') }}\
                "
    })
}}
select 1 as id
"""


post = """
select 'end' as test_state
"""


pre = """
select 'start' as test_state
"""
