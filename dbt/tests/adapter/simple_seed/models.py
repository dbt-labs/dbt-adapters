downstream_from_seed_actual = """
select * from {{ ref('seed_actual') }}

"""


downstream_from_seed_pipe_separated = """
select * from {{ ref('seed_pipe_separated') }}

"""


from_basic_seed = """
select * from {{ this.schema }}.seed_expected

"""