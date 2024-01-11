import os

import argparse
import sys

import anyio as anyio
import dagger as dagger
from dotenv import find_dotenv, load_dotenv

PG_PORT = 5432
load_dotenv(find_dotenv("test.env"))
# if env vars aren't specified in test.env (i.e. in github actions worker), use the ones from the host
TESTING_ENV_VARS = {
    env_name: os.environ[env_name]
    for env_name in os.environ
    if env_name.startswith(("DD_", "DBT_"))
}

TESTING_ENV_VARS.update({"ODBC_DRIVER": "/opt/simba/spark/lib/64/libsparkodbc_sb64.so"})


def env_variables(envs: dict[str, str]):
    def env_variables_inner(ctr: dagger.Container):
        for key, value in envs.items():
            ctr = ctr.with_env_variable(key, value)
        return ctr

    return env_variables_inner


async def get_postgres_container(client: dagger.Client) -> (dagger.Container, str):
    ctr = await (
        client.container()
        .from_("postgres:13")
        .with_env_variable("POSTGRES_PASSWORD", "postgres")
        .with_exposed_port(PG_PORT)
    )

    return ctr, "postgres_db"


async def get_spark_container(client: dagger.Client) -> (dagger.Container, str):
    spark_dir = client.host().directory("./dagger/spark-container")
    spark_ctr_base = (
        client.container()
        .from_("eclipse-temurin:8-jre")
        .with_directory("/spark_setup", spark_dir)
        .with_env_variable("SPARK_HOME", "/usr/spark")
        .with_env_variable("PATH", "/usr/spark/bin:/usr/spark/sbin:$PATH", expand=True)
        .with_file(
            "/scripts/entrypoint.sh",
            client.host().file("./dagger/spark-container/entrypoint.sh"),
            permissions=755,
        )
        .with_file(
            "/scripts/install_spark.sh",
            client.host().file("./dagger/spark-container/install_spark.sh"),
            permissions=755,
        )
        .with_exec(["./spark_setup/install_spark.sh"])
        .with_file("/usr/spark/conf/hive-site.xml", spark_dir.file("/hive-site.xml"))
        .with_file("/usr/spark/conf/spark-defaults.conf", spark_dir.file("spark-defaults.conf"))
    )

    # postgres is the metastore here
    pg_ctr, pg_host = await get_postgres_container(client)

    spark_ctr = (
        spark_ctr_base.with_service_binding(alias=pg_host, service=pg_ctr)
        .with_exec(
            [
                "/scripts/entrypoint.sh",
                "--class",
                "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2",
                "--name",
                "Thrift JDBC/ODBC Server",
            ]
        )
        .with_exposed_port(10000)
    )

    return spark_ctr, "spark_db"


async def test_spark(test_args):
    async with dagger.Connection(dagger.Config(log_output=sys.stderr)) as client:
        test_profile = test_args.profile
        req_files = client.host().directory("./", include=["*.txt", "*.env", "*.ini"])
        dbt_spark_dir = client.host().directory("./dbt")
        test_dir = client.host().directory("./tests")
        scripts = client.host().directory("./dagger/scripts")
        platform = dagger.Platform("linux/amd64")
        tst_container = (
            client.container(platform=platform)
            .from_("python:3.8-slim")
            .with_directory("/.", req_files)
            .with_directory("/dbt", dbt_spark_dir)
            .with_directory("/tests", test_dir)
            .with_directory("/scripts", scripts)
            .with_exec("./scripts/install_os_reqs.sh")
            .with_exec(["pip", "install", "-r", "requirements.txt"])
            .with_exec(["pip", "install", "-r", "dev-requirements.txt"])
        )

        if test_profile == "apache_spark":
            spark_ctr, spark_host = await get_spark_container(client)
            tst_container = tst_container.with_service_binding(alias=spark_host, service=spark_ctr)

        elif test_profile in ["databricks_cluster", "databricks_sql_endpoint"]:
            tst_container = tst_container.with_exec("./scripts/configure_odbc.sh")

        elif test_profile == "spark_session":
            tst_container = tst_container.with_exec(["pip", "install", "pyspark"])
            tst_container = tst_container.with_exec(["apt-get", "install", "openjdk-17-jre", "-y"])

        tst_container = tst_container.with_(env_variables(TESTING_ENV_VARS))
        test_path = test_args.test_path if test_args.test_path else "tests/functional/adapter"
        result = await tst_container.with_exec(
            ["pytest", "-v", "--profile", test_profile, "-n", "auto", test_path]
        ).stdout()

        return result


parser = argparse.ArgumentParser()
parser.add_argument("--profile", required=True, type=str)
parser.add_argument("--test-path", required=False, type=str)
args = parser.parse_args()

anyio.run(test_spark, args)
