import json
import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

# from src.utility.validation import write_output, load_credentials
from pathlib import Path
import yaml


def load_credentials(path: str = None) -> dict:
    """
    Load credentials from YAML file.
    If `path` is provided, use it directly.
    Otherwise, load from <project_root>/config/cred_config.yml.
    """
    if path:
        config_path = Path(path)
    else:
        # __file__ is tests/conftest.py → parent is tests → parent.parent is project root
        project_root = Path(__file__).resolve().parent.parent
        config_path = project_root / "config" / "cred_config.yml"
    if not config_path.exists():
        raise FileNotFoundError(f"❌ Credentials file not found at: {config_path}")

    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


@pytest.fixture(scope='session')
def spark_session(request):
    # Placeholder for required JARs (JDBC drivers, etc.)
    base_dir = Path(__file__).resolve().parents[1]
    jars_dir = base_dir/'jars'
    snow_jar = jars_dir/"snowflake-jdbc-3.25.0.jar"
    postgrace_jar = ""
    azure_storage = jars_dir/"azure-storage-8.6.6.jar"
    hadoop_azure = jars_dir/"hadoop-azure-3.3.1.jar"
    oracle_jar = jars_dir/"ojdbc6.jar"
    jar_path = ",".join(str(j) for j in [snow_jar, azure_storage, hadoop_azure, oracle_jar])
    spark = SparkSession.builder.master('local[2]').appName('etl_validation') \
        .config('spark.jars', jar_path) \
        .config("spark.executor.extraClassPath", jar_path) \
        .config("spark.driver.extraClassPath", jar_path) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    adls_account_name = 'augauto'
    adls_container_name = 'raw'
    key = "8Qtx7EgdUQ9JSWkmNWNgwdyMFQamCOIrE0fxBgGkvJNJG/1+3UjYw1w+ZcK3Qmf+7Q0vEMUxqVfQ+AStaut73Q=="
    spark.conf.set(f"fs.azure.account.auth.type.{adls_account_name}.dfs.core.windows.net","SharedKey")
    spark.conf.set(f"fs.azure.account.key.{adls_account_name}.dfs.core.windows.net", key)
    return spark

def test_1(spark_session):
    print(spark_session.createDataFrame([('soma', '30'), ('priti','31')], schema=('name STRING', 'age STRING')))


@pytest.fixture(scope='module')
def read_config(request):
    dir_path = request.node.fspath.dirname
    config_path = dir_path + '\\config.yml'
    with open(config_path, 'r') as f:
        config_data = yaml.safe_load(f)
    return config_data


def read_schema(dir_path):
    schema_path = dir_path + '\\schema.json'
    with open(schema_path, 'r') as schema_file:
        schema = StructType.fromJson(json.load(schema_file))
    return schema


def read_query(dir_path):
    sql_query_path = dir_path + '\\transformation.sql'
    with open(sql_query_path, 'r') as file:
        sql_query = file.read()
    return sql_query


def read_file(config_data, spark, dir_path=None):
    if config_data['type'] == 'csv':
        if config_data.get('schema', '').upper() == 'Y':
            schema = read_schema(dir_path)
            df = spark.read.schema(schema).csv(config_data['path'], header=config_data['options'].get('header', 'false'))
        else:
            df = spark.read.csv(config_data['path'], header=True, inferSchema=True)
    elif config_data['type'] == 'json':
        df = spark.read.json(config_data['path'], multiline=config_data['options'].get('multiline', 'false'))
    elif config_data['type'] == 'parquet':
        df = spark.read.parquet(config_data['path'])
    else:
        raise ValueError(f"Unsupported file type: {config_data['type']}")
    return df


def read_db(config_data, spark, dir_path):
    creds = load_credentials()
    cred_lookup = config_data['cred_lookup']
    creds = creds.get(cred_lookup, {})
    if config_data.get('transformation_sql', '').upper() == 'Y':
        sql_query = read_query(dir_path)
        df = spark.read.format('jdbc') \
            .option('url', creds.get('url')) \
            .option('user', creds.get('user')) \
            .option('password', creds.get('password')) \
            .option('query', sql_query) \
            .option('driver', creds.get('driver')).load()
    else:
        df = spark.read.format('jdbc') \
            .option('url', creds.get('url')) \
            .option('user', creds.get('user')) \
            .option('password', creds.get('password')) \
            .option('dbtable', config_data['table']) \
            .option('driver', creds.get('driver')).load()
    return df


@pytest.fixture(scope='module')
def read_data(read_config, spark_session, request):
    spark = spark_session
    config_data = read_config
    source_config = config_data['source']
    target_config = config_data['target']
    dir_path = request.node.fspath.dirname
    # Read source
    if source_config['type'] == 'database':
        source_df = read_db(source_config, spark, dir_path)
    else:
        source_df = read_file(source_config, spark, dir_path)
    # Read target
    if target_config['type'] == 'database':
        target_df = read_db(target_config, spark, dir_path)
    else:
        target_df = read_file(target_config, spark, dir_path)
    return source_df, target_df

