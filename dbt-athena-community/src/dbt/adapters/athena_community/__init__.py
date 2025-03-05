# this is a shell package that allows us to publish dbt-athena as dbt-athena-community
import warnings

warnings.warn(
    "dbt-athena-community will be deprecated in favor of dbt-athena following version 1.9.x. To continue using new features, please run `pip install dbt-athena` instead.",
    DeprecationWarning,
)
