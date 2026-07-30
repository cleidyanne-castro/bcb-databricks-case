from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


RAW_SCHEMA = StructType(
    [
        StructField("data", StringType(), True),
        StructField("valor", StringType(), True),
    ]
)


def read_bronze_stream(
    spark: SparkSession,
    source_path: str,
    series_name: str,
) -> DataFrame:
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .schema(RAW_SCHEMA)
        .load(source_path)
        .select(
            F.col("data").alias("data_raw"),
            F.col("valor").alias("valor_raw"),
            F.lit(series_name).alias("series_name"),
            F.col("_metadata.file_name").alias("source_file"),
            F.col("_metadata.file_path").alias("source_path"),
            F.current_timestamp().alias("ingestion_timestamp"),
            F.current_date().alias("ingestion_date"),
        )
    )


def write_bronze_stream(
    dataframe: DataFrame,
    target_table: str,
    checkpoint_path: str,
) -> None:
    query = (
        dataframe.writeStream
        .format("delta")
        .option(
            "checkpointLocation",
            checkpoint_path,
        )
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(target_table)
    )

    query.awaitTermination()


def ingest_series(
    spark: SparkSession,
    source_path: str,
    series_name: str,
    target_table: str,
    checkpoint_path: str,
) -> None:
    dataframe = read_bronze_stream(
        spark=spark,
        source_path=source_path,
        series_name=series_name,
    )

    write_bronze_stream(
        dataframe=dataframe,
        target_table=target_table,
        checkpoint_path=checkpoint_path,
    )


def run_bronze(spark: SparkSession) -> None:
    volume_path = "/Volumes/beanalytic_case/landing/raw_files"

    ingest_series(
        spark=spark,
        source_path=f"{volume_path}/selic",
        series_name="selic",
        target_table="beanalytic_case.bronze.selic",
        checkpoint_path=(
            f"{volume_path}/checkpoints/bronze_selic"
        ),
    )

    ingest_series(
        spark=spark,
        source_path=f"{volume_path}/ipca",
        series_name="ipca",
        target_table="beanalytic_case.bronze.ipca",
        checkpoint_path=(
            f"{volume_path}/checkpoints/bronze_ipca"
        ),
    )